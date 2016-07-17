package main

import (
	"net/http"
	"encoding/json"
	"fmt"
	"strings"
	"github.com/boltdb/bolt"
	"time"
)

//API_URL Base url for Api calls
const (
	API_URL string = "http://torpedo.servegame.com:3000"
	BOLT_BUCKET_NAME string = "songs"
	BOLT_DB_FILE = "songs.db"
)

//TopSongResp Response for receiving top billboard song /getTopSongs
type TopSongResp struct {
	Name string `json:"name"`
	Artist string `json:"artist"`
}

//SongResp Response received when /v2/download/:query is done
type SongResp struct {
	Success bool `json:"success`
	Url string `json:"url"`
}

//SongInfo overall data collected for a particular song
type SongInfo struct {
	Name string `json:"name"`
	Artist string `json:"artist"`
	Url string `json:"url"`
}

// get top billboard songs
func getTopSongs() ([]TopSongResp, error) {
	res, err := http.Get(API_URL + "/getTopSongs")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	var songs []TopSongResp

	err = decoder.Decode(&songs)
	if err != nil {
		return nil,err
	}
	
	return songs, nil
}

// strip and format the parameter. used because the api gives unformatted data
func cleanArgs(arg *string) {
	*arg = strings.TrimSpace(strings.Split(*arg, "\n")[1])
}

// use /v2/download to download a song to s3
func downloadToS3(song *TopSongResp) (*SongInfo, error) {
	cleanArgs(&song.Artist);

	query := song.Name + song.Artist
	res, err := http.Get(API_URL + "/v2/download/" + query)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	var songResp SongResp

	err = decoder.Decode(&songResp)
	if err != nil && songResp.Success == false{
		return nil, err
	}

	var s SongInfo
	s.Name = song.Name
	s.Artist = song.Artist
	s.Url = songResp.Url

	return &s, nil
}

// Save the song in the db
func persist(song *SongInfo, db *bolt.DB) error {
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
				fmt.Println("Error decoding db value for key " + string(k) )
			}

			songs = append(songs, song)
		}

		return nil
	})

	return songs, err
}

func updateBillBoardData(db *bolt.DB) {
	songs, err := getTopSongs();
	if err != nil {
		panic(err.Error())
	}

	for _, song := range songs  {
		song, err := downloadToS3(&song)
		if err != nil {
			fmt.Errorf("%s Download Error occured for %s", err.Error(), song.Name)
		}

		err = persist(song, db)
		if err != nil {
			fmt.Errorf("%s Persist Error occured for %s", err.Error(), song.Name)
		}
		break
	}
}

func main() {
	db,err := bolt.Open(BOLT_DB_FILE, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	updateBillBoardData(db)

	http.HandleFunc("/billboard", func (w http.ResponseWriter, r *http.Request) {
		songs, err := getTopSongUrl(db)
		if err != nil {
			json.NewEncoder(w).Encode(err)
		}

		json.NewEncoder(w).Encode(songs)
	})
	http.ListenAndServe(":8000", nil)
}
