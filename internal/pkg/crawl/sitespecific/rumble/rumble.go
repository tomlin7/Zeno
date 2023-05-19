package rumble

import "time"

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
