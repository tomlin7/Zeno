package rumble

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/CorentinB/warc"
	"github.com/PuerkitoBio/goquery"
)

type EmbedJS struct {
	Fps float64 `json:"fps"`
	W   int     `json:"w"`
	H   int     `json:"h"`
	U   struct {
		Mp4 struct {
			URL  string `json:"url"`
			Meta struct {
				Bitrate int `json:"bitrate"`
				Size    int `json:"size"`
				W       int `json:"w"`
				H       int `json:"h"`
			} `json:"meta"`
		} `json:"mp4"`
		Timeline struct {
			URL  string `json:"url"`
			Meta struct {
				Bitrate int `json:"bitrate"`
				Size    int `json:"size"`
				W       int `json:"w"`
				H       int `json:"h"`
			} `json:"meta"`
		} `json:"timeline"`
	} `json:"u"`
	Ua struct {
		Mp4 struct {
			Num240 struct {
				URL  string `json:"url"`
				Meta struct {
					Bitrate int `json:"bitrate"`
					Size    int `json:"size"`
					W       int `json:"w"`
					H       int `json:"h"`
				} `json:"meta"`
			} `json:"240"`
			Num360 struct {
				URL  string `json:"url"`
				Meta struct {
					Bitrate int `json:"bitrate"`
					Size    int `json:"size"`
					W       int `json:"w"`
					H       int `json:"h"`
				} `json:"meta"`
			} `json:"360"`
			Num480 struct {
				URL  string `json:"url"`
				Meta struct {
					Bitrate int `json:"bitrate"`
					Size    int `json:"size"`
					W       int `json:"w"`
					H       int `json:"h"`
				} `json:"meta"`
			} `json:"480"`
			Num720 struct {
				URL  string `json:"url"`
				Meta struct {
					Bitrate int `json:"bitrate"`
					Size    int `json:"size"`
					W       int `json:"w"`
					H       int `json:"h"`
				} `json:"meta"`
			} `json:"720"`
			Num1080 struct {
				URL  string `json:"url"`
				Meta struct {
					Bitrate int `json:"bitrate"`
					Size    int `json:"size"`
					W       int `json:"w"`
					H       int `json:"h"`
				} `json:"meta"`
			} `json:"1080"`
		} `json:"mp4"`
		Timeline struct {
			Num180 struct {
				URL  string `json:"url"`
				Meta struct {
					Bitrate int `json:"bitrate"`
					Size    int `json:"size"`
					W       int `json:"w"`
					H       int `json:"h"`
				} `json:"meta"`
			} `json:"180"`
		} `json:"timeline"`
	} `json:"ua"`
	I string `json:"i"`
	T []struct {
		I string `json:"i"`
		W int    `json:"w"`
		H int    `json:"h"`
	} `json:"t"`
	Evt struct {
		V  string `json:"v"`
		E  string `json:"e"`
		Wt int    `json:"wt"`
		T  string `json:"t"`
	} `json:"evt"`
	Cc     []any  `json:"cc"`
	L      string `json:"l"`
	R      int    `json:"r"`
	Title  string `json:"title"`
	Author struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"author"`
	Player           bool      `json:"player"`
	Duration         int       `json:"duration"`
	PubDate          time.Time `json:"pubDate"`
	Loaded           int       `json:"loaded"`
	Vid              int       `json:"vid"`
	Timeline         []int     `json:"timeline"`
	Own              bool      `json:"own"`
	Mod              []any     `json:"mod"`
	Restrict         []int     `json:"restrict"`
	Autoplay         int       `json:"autoplay"`
	Track            int       `json:"track"`
	Live             int       `json:"live"`
	LivePlaceholder  bool      `json:"live_placeholder"`
	LivestreamHasDvr any       `json:"livestream_has_dvr"`
	A                struct {
		Timeout int    `json:"timeout"`
		U       string `json:"u"`
		Aden    []int  `json:"aden"`
		Ov      bool   `json:"ov"`
		Ads     []any  `json:"ads"`
		A       string `json:"a"`
		Ae      string `json:"ae"`
		Ap      []any  `json:"ap"`
		Loop    []any  `json:"loop"`
	} `json:"a"`
}

// func IsVideo(URL string) {
// 	// For an URL to be matched as a video, it must match the following regex:
// 	// https://rumble.com/v[0-9a-zA-Z]+
// 	// https://rumble.com/embed/[0-9a-zA-Z]+
// 	// https://rumble.com/c-[0-9a-zA-Z]+
// 	// https://rumble.com/user/[0-9a-zA-Z]+
// 	// https://rumble.com/v[0-9a-zA-Z]+.html
// 	// https://rumble.com/embed/[0-9a-zA-Z]+.html
// 	// https://rumble.com/c-[0-9a-zA-Z]+.html
// 	// https://rumble.com/user/[0-9a-zA-Z]+.html
// 	// https://rumble.com/v[0-9a-zA-Z]+.html?mref=
// 	// https://rumble.com/embed/[0-9a-zA-Z]+.html?mref=

// }

func GetVideoURLs(doc *goquery.Document, httpClient *warc.CustomHTTPClient) (videoURLs []url.URL, err error) {
	// Look for the oembed URL to find the video ID
	oembed := findOEmbed(doc)
	if oembed == "" {
		return
	}

	// Get the video ID from the oembed URL (last element of the URL, after the last /)
	videoID := oembed[strings.LastIndex(oembed, "/")+1:]
	if videoID == "" {
		return
	}

	// Get the video data from the API
	embedJS := EmbedJS{}

	req, err := http.NewRequest("GET", "https://rumble.com/embedJS/u3/?request=video&ver=2&v="+videoID+"&ext=%7B%22ad_count%22%3Anull%7D&ad_wt=0", nil)
	if err != nil {
		return
	}

	req.Header.Set("Authority", "rumble.com")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Referer", utils.URLToString(doc.Url))
	req.Header.Set("Sec-Ch-Ua", "\"Google Chrome\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"")
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", "\"Linux\"")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")

	// Execute the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}

	// Parse the body as an EmbedJS struct
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	embedJS = EmbedJS{}
	err = json.Unmarshal(body, &embedJS)
	if err != nil {
		return
	}

	// Get the video URLs
	var rawURLs []string

	rawURLs = append(rawURLs,
		embedJS.I,
		embedJS.U.Mp4.URL,
		embedJS.U.Timeline.URL,
		embedJS.Ua.Mp4.Num240.URL,
		embedJS.Ua.Mp4.Num360.URL,
		embedJS.Ua.Mp4.Num480.URL,
		embedJS.Ua.Mp4.Num720.URL,
		embedJS.Ua.Mp4.Num1080.URL,
		embedJS.Ua.Timeline.Num180.URL,
	)

	for _, rawURL := range rawURLs {
		videoURL, err := url.Parse(rawURL)
		if err != nil {
			continue
		}

		videoURLs = append(videoURLs, *videoURL)
	}

	return
}

func findOEmbed(doc *goquery.Document) string {
	oembed, _ := doc.Find("link[rel='alternate'][type='application/json+oembed']").Attr("href")
	return oembed
}
