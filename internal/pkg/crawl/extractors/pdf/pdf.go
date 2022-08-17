package pdf

import (
	"bytes"
	"fmt"
	"io"
	"net/url"

	"github.com/ledongthuc/pdf"
	"github.com/sirupsen/logrus"
)

func ExtractURLsFromText(body io.ReadCloser) (URLs []*url.URL, err error) {
	fmt.Println("Called")

	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return URLs, err
	}

	f, r, err := pdf.NewReader()
	// remember close file
	defer f.Close()
	if err != nil {
		return URLs, err
	}

	var buf bytes.Buffer
	b, err := r.GetPlainText()
	if err != nil {
		return URLs, err
	}
	buf.ReadFrom(b)

	logrus.Info(buf.String())

	return URLs, nil
}
