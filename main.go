package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/jasonlvhit/gocron"
)

//API_URL Base url for Api calls
const (
	API_URL          string = "http://torpedo.servegame.com:3000"
	BOLT_BUCKET_NAME string = "songs"
	BOLT_DB_FILE            = "songs.db"
)

//TopSongResp Response for receiving top billboard song /getTopSongs
type TopSongResp struct {
	Name   string `json:"name"`
	Artist string `json:"artist"`
}

//SongResp Response received when /v2/download/:query is done
type SongResp struct {
	Success bool   `json:"success`
	Url     string `json:"url"`
}

//SongInfo overall data collected for a particular song
type SongInfo struct {
	Name   string `json:"name"`
	Artist string `json:"artist"`
	Url    string `json:"url"`
}

// get top billboard songs
func getTopSongs() ([]TopSongResp, error) {
	log.Println("Retreiving top billboard songs")

	res, err := http.Get(API_URL + "/getTopSongs")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	var songs []TopSongResp

	err = decoder.Decode(&songs)
	if err != nil {
		return nil, err
	}

	return songs, nil
}

// strip and format the parameter. used because the api gives unformatted data
func cleanArgs(arg *string) {
	*arg = strings.TrimSpace(strings.Split(*arg, "\n")[1])
}

// use /v2/download to download a song to s3
func downloadToS3(song *TopSongResp, songChan chan SongInfo) {
	log.Println("Initiate search and download to s3 for " + song.Name)

	cleanArgs(&song.Artist)

	query := song.Name + song.Artist
	res, err := http.Get(API_URL + "/v2/download/" + query)
	if err != nil {
		fmt.Println("Error " + err.Error())
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	var songResp SongResp

	err = decoder.Decode(&songResp)
	if err != nil && songResp.Success == false {
		fmt.Println("Error " + err.Error())
	}

	var s SongInfo
	s.Name = song.Name
	s.Artist = song.Artist
	s.Url = songResp.Url

	songChan <- s
}

// Save the song in the db
func persist(song *SongInfo, db *bolt.DB) error {
	log.Println("Saving the song in db " + song.Name)

	return db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(BOLT_BUCKET_NAME))
		if err != nil {
			return err
		}

		encoded, err := json.Marshal(song)
		if err != nil {
			return err
		}

		b.Put([]byte(song.Name), encoded)
		return nil
	})
}

// retrieve all songs saved in the db
func getTopSongUrl(db *bolt.DB) ([]SongInfo, error) {
	var songs []SongInfo

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLT_BUCKET_NAME))

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var song SongInfo
			err := json.Unmarshal(v, &song)
			if err != nil {
				fmt.Println("Error decoding db value for key " + string(k))
			}

			songs = append(songs, song)
		}

		return nil
	})

	return songs, err
}

// perform billboard mgmt
func updateBillBoardData(db *bolt.DB) error {
	log.Println("Starting billboard update")

	err := db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(BOLT_BUCKET_NAME))
	})
	if err != nil {
		panic(err.Error())
	}
	log.Println("Existing data deleted")

	songs, err := getTopSongs()
	if err != nil {
		panic(err.Error())
	}

	songChan := make(chan  SongInfo, 100)

	go func() {
		for _, song := range songs {
			downloadToS3(&song, songChan)
		}
		close(songChan)
	}()

	for song := range songChan {
		go persist(&song, db)
	}

	log.Println("Finished billboard update")
	return nil
}

func scheduleCron (db *bolt.DB) {
	log.Println("Scheduling CRON Job")
	gocron.Every(1).Wednesday().Do(updateBillBoardData, db)
	<-gocron.Start()
}

func main() {
	db, err := bolt.Open(BOLT_DB_FILE, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BOLT_BUCKET_NAME))
		return err
	})

	go updateBillBoardData(db)
	go scheduleCron(db)

	http.HandleFunc("/billboard", func(w http.ResponseWriter, r *http.Request) {
		songs, err := getTopSongUrl(db)
		if err != nil {
			json.NewEncoder(w).Encode(err)
		}

		json.NewEncoder(w).Encode(songs)
	})

	log.Println("Server starting on port :8000 and route /billboard")
	err = http.ListenAndServe(":8000", nil)
	if err != nil {
		panic(err.Error())
	}
}
