/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

// The idea is that a heka configured like:
//
// [S3SplitFileInput]
// (configured to point at landfill)
// [LandfillHttpOutput]
// message_matcher = "Type == 'heka.httpdata.request'"
// # message_matcher = "TRUE"
// address = "https://STAGE_URL"
// encoder = "PayloadEncoder"
//
// is a pretty decent approximation of real traffic.

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins/tcp"
)

type LandfillHttpOutput struct {
	*LandfillHttpOutputConfig
	url          *url.URL
	client       *http.Client
	useBasicAuth bool
	sendBody     bool
	pathVariable *messageVariable
}

type LandfillHttpOutputConfig struct {
	LandfillHttpTimeout uint32 `toml:"http_timeout"`
	Address             string
	Method              string
	Headers             []string
	Username            string `toml:"username"`
	Password            string `toml:"password"`
	Tls                 tcp.TlsConfig
}

func (o *LandfillHttpOutput) ConfigStruct() interface{} {
	return &LandfillHttpOutputConfig{
		LandfillHttpTimeout: 0,
		Headers:             []string{"ContentType", "Content-Length", "User-Agent"},
		Method:              "POST",
	}
}

func (o *LandfillHttpOutput) Init(config interface{}) (err error) {
	o.LandfillHttpOutputConfig = config.(*LandfillHttpOutputConfig)
	if o.url, err = url.Parse(o.Address); err != nil {
		return fmt.Errorf("Can't parse URL '%s': %s", o.Address, err.Error())
	}
	if o.url.Scheme != "http" && o.url.Scheme != "https" {
		return errors.New("`address` must contain an absolute http or https URL.")
	}
	o.Method = strings.ToUpper(o.Method)
	if o.Method != "POST" && o.Method != "GET" && o.Method != "PUT" {
		return errors.New("HTTP Method must be POST, GET, or PUT.")
	}
	if o.Method != "GET" {
		o.sendBody = true
	}
	o.client = new(http.Client)
	if o.LandfillHttpTimeout > 0 {
		o.client.Timeout = time.Duration(o.LandfillHttpTimeout) * time.Millisecond
	}
	if o.Username != "" || o.Password != "" {
		o.useBasicAuth = true
	}
	if o.url.Scheme == "https" {
		transport := &http.Transport{}
		if transport.TLSClientConfig, err = tcp.CreateGoTlsConfig(&o.Tls); err != nil {
			return fmt.Errorf("TLS init error: %s", err.Error())
		}
		o.client.Transport = transport
	}
	o.pathVariable = verifyMessageVariable("Fields[Path]")

	return
}

func (o *LandfillHttpOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	if or.Encoder() == nil {
		return errors.New("Encoder must be specified.")
	}

	var (
		e        error
		outBytes []byte
	)
	inChan := or.InChan()

	for pack := range inChan {
		outBytes, e = or.Encode(pack)
		if e != nil {
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(fmt.Errorf("can't encode: %s", e))
			continue
		}
		if outBytes == nil {
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
			continue
		}
		if e = o.request(or, outBytes, pack.Message); e != nil {
			e = pipeline.NewRetryMessageError(e.Error())
			pack.Recycle(e)
		} else {
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
		}
	}

	return
}

func (o *LandfillHttpOutput) request(or pipeline.OutputRunner, outBytes []byte, msg *message.Message) (err error) {
	path := getMessageVariable(msg, o.pathVariable)
	// we add back the query parameter to ensure the tee routes the data appropriately
	u := o.Address + path + "?v=4"

	var parsed *url.URL
	if parsed, err = url.Parse(u); err != nil {
		return fmt.Errorf("Can't parse URL '%s': %s", o.Address, err.Error())
	}

	// Fields: [
	// name:"Content-Length" value_string:"12483"
	// name:"UserAgent" value_string:"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0"
	// name:"ContentType" value_string:"application/json; charset=UTF-8"
	// name:"Host" value_string:"incoming.telemetry.mozilla.org"
	// name:"geoCountry" value_string:"BD"
	// name:"Path" value_string:"/submit/telemetry/uuid/saved-session/Firefox/39.0/beta/20150626112833"
	// name:"Protocol" value_string:"HTTP/1.1"
	// ]

	var (
		resp       *http.Response
		reader     io.Reader
		readCloser io.ReadCloser
	)
	header := make(http.Header)
	for _, h := range o.Headers {
		if value := msg.FindFirstField(h); value != nil && value.GetValueType() == message.Field_STRING {
			header.Add(h, value.ValueString[0])
		}
	}

	req := &http.Request{
		Method: o.Method,
		URL:    parsed,
		Header: header,
	}

	if o.useBasicAuth {
		req.SetBasicAuth(o.Username, o.Password)
	}

	if o.sendBody {
		req.ContentLength = int64(len(outBytes))
		reader = bytes.NewReader(outBytes)
		readCloser = ioutil.NopCloser(reader)
		req.Body = readCloser
	}
	if resp, err = o.client.Do(req); err != nil {
		return fmt.Errorf("Error making HTTP request: %s", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Error reading HTTP response: %s", err.Error())
		}
		return fmt.Errorf("HTTP Error code returned: %d %s - %s",
			resp.StatusCode, resp.Status, string(body))
	}
	return
}

func init() {
	pipeline.RegisterPlugin("LandfillHttpOutput", func() interface{} {
		return new(LandfillHttpOutput)
	})
}

// ripped from KafkaOutput
var fieldRegex = regexp.MustCompile("^Fields\\[([^\\]]*)\\](?:\\[(\\d+)\\])?(?:\\[(\\d+)\\])?$")

type messageVariable struct {
	header bool
	name   string
	fi     int
	ai     int
}

func verifyMessageVariable(key string) *messageVariable {
	switch key {
	case "Type", "Logger", "Hostname", "Payload":
		return &messageVariable{header: true, name: key}
	default:
		matches := fieldRegex.FindStringSubmatch(key)
		if len(matches) == 4 {
			mvar := &messageVariable{header: false, name: matches[1]}
			if len(matches[2]) > 0 {
				if parsedInt, err := strconv.ParseInt(matches[2], 10, 32); err == nil {
					mvar.fi = int(parsedInt)
				} else {
					return nil
				}
			}
			if len(matches[3]) > 0 {
				if parsedInt, err := strconv.ParseInt(matches[3], 10, 32); err == nil {
					mvar.ai = int(parsedInt)
				} else {
					return nil
				}
			}
			return mvar
		}
		return nil
	}
}

func getFieldAsString(msg *message.Message, mvar *messageVariable) string {
	var field *message.Field
	if mvar.fi != 0 {
		fields := msg.FindAllFields(mvar.name)
		if mvar.fi >= len(fields) {
			return ""
		}
		field = fields[mvar.fi]
	} else {
		if field = msg.FindFirstField(mvar.name); field == nil {
			return ""
		}
	}
	switch field.GetValueType() {
	case message.Field_STRING:
		if mvar.ai >= len(field.ValueString) {
			return ""
		}
		return field.ValueString[mvar.ai]
	case message.Field_BYTES:
		if mvar.ai >= len(field.ValueBytes) {
			return ""
		}
		return string(field.ValueBytes[mvar.ai])
	case message.Field_INTEGER:
		if mvar.ai >= len(field.ValueInteger) {
			return ""
		}
		return fmt.Sprintf("%d", field.ValueInteger[mvar.ai])
	case message.Field_DOUBLE:
		if mvar.ai >= len(field.ValueDouble) {
			return ""
		}
		return fmt.Sprintf("%g", field.ValueDouble[mvar.ai])
	case message.Field_BOOL:
		if mvar.ai >= len(field.ValueBool) {
			return ""
		}
		return fmt.Sprintf("%t", field.ValueBool[mvar.ai])
	}
	return ""
}

func getMessageVariable(msg *message.Message, mvar *messageVariable) string {
	if mvar.header {
		switch mvar.name {
		case "Type":
			return msg.GetType()
		case "Logger":
			return msg.GetLogger()
		case "Hostname":
			return msg.GetHostname()
		case "Payload":
			return msg.GetPayload()
		default:
			return ""
		}
	} else {
		return getFieldAsString(msg, mvar)
	}
}
