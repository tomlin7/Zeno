package utils

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"runtime"
	"strings"
	"unsafe"

	"github.com/asaskevich/govalidator"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"golang.org/x/net/idna"
)

func URLToString(u *url.URL) string {
	var err error

	logger, created := log.DefaultOrStored()
	if created {
		panic("logger not initialized")
	}
	if u != nil {
		// Get the size of the URL object
		size := int(unsafe.Sizeof(*u))

		// Add sizes of string fields
		size += len(u.Scheme)
		size += len(u.Opaque)
		size += len(u.Host)
		size += len(u.Path)
		size += len(u.RawPath)
		size += len(u.RawQuery)
		size += len(u.Fragment)
		size += len(u.RawFragment)

		// Add size of User info if present
		if u.User != nil {
			username := u.User.Username()
			size += len(username)
			password, hasPassword := u.User.Password()
			if hasPassword {
				size += len(password)
			}
		}

		// Get caller information
		pc, file, line, ok := runtime.Caller(1)
		caller := "unknown"
		if ok {
			fn := runtime.FuncForPC(pc)
			caller = fmt.Sprintf("%s (%s:%d)", fn.Name(), file, line)
		}

		logger.Info("URL object size", "size", size, "url", u.String(), "caller", caller)
	}

	q := u.Query()
	u.RawQuery = q.Encode()
	u.Host, err = idna.ToASCII(u.Host)
	if err != nil {
		if strings.Contains(u.Host, ":") {
			hostWithoutPort, port, err := net.SplitHostPort(u.Host)
			if err != nil {
				slog.Warn("cannot split host and port", "error", err)
			} else {
				asciiHost, err := idna.ToASCII(hostWithoutPort)
				if err == nil {
					u.Host = asciiHost + ":" + port
				} else {
					slog.Warn("cannot encode punycode host without port to ASCII", "error", err)
				}
			}
		} else {
			slog.Warn("cannot encode punycode host to ASCII", "error", err)
		}
	}

	return u.String()
}

// MakeAbsolute turn all URLs in a slice of url.URL into absolute URLs, based
// on a given base *url.URL
func MakeAbsolute(base *url.URL, URLs []*url.URL) []*url.URL {
	for i, URL := range URLs {
		if !URL.IsAbs() {
			URLs[i] = base.ResolveReference(URL)
		}
	}

	return URLs
}

func RemoveFragments(URLs []*url.URL) []*url.URL {
	for i := range URLs {
		URLs[i].Fragment = ""
	}

	return URLs
}

// DedupeURLs take a slice of *url.URL and dedupe it
func DedupeURLs(URLs []*url.URL) []*url.URL {
	keys := make(map[string]bool)
	list := []*url.URL{}

	for _, entry := range URLs {
		if _, value := keys[URLToString(entry)]; !value {
			keys[URLToString(entry)] = true

			if entry.Scheme == "http" || entry.Scheme == "https" {
				list = append(list, entry)
			}
		}
	}

	return list
}

// ValidateURL validates a *url.URL
func ValidateURL(u *url.URL) error {
	valid := govalidator.IsURL(URLToString(u))

	if u.Scheme != "http" && u.Scheme != "https" {
		valid = false
	}

	if !valid {
		return errors.New("not a valid URL")
	}

	return nil
}
