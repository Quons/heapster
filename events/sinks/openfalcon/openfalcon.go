// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package openfalcon

import (
	"bytes"
	"fmt"

	"encoding/base64"
	"encoding/json"
	"github.com/golang/glog"
	"io/ioutil"
	"k8s.io/heapster/events/core"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type openfalconSink struct {
	//这里类似于继承RWMutex
	sync.RWMutex
	host string
	// wg and conChan will work together to limit concurrent influxDB sink goroutines.
	wg sync.WaitGroup
	//使用到了空结构体作为chan存储类型
	conChan chan struct{}
}

type falconType struct {
	Endpoint    string      `json:"endpoint"`
	Metric      string      `json:"metric"`
	Timestamp   int64       `json:"timestamp"`
	Step        int         `json:"step"`
	Value       interface{} `json:"value"`
	CounterType string      `json:"counterType"`
	Tags        string      `json:"tags"`
}

func (this *openfalconSink) Name() string {
	return "openfalconSink"
}

func (this *openfalconSink) Stop() {
	// Do nothing.
}

func batchToString(batch *core.EventBatch) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("EventBatch     Timestamp: %s\n", batch.Timestamp))
	for _, event := range batch.Events {
		buffer.WriteString(fmt.Sprintf("   %s (cnt:%d): %s\n", event.LastTimestamp, event.Count, event.Message))
	}
	return buffer.String()
}

func (this *openfalconSink) ExportEvents(batch *core.EventBatch) {
	if len(batch.Events) == 0 {
		return
	}
	ebJson, err := json.Marshal(batch.Events)
	glog.V(3).Infof("%s", ebJson)
	if err != nil {
		glog.Errorf("marshal event to json error:%s", err)
		return
	}
	//base64编码ebJson
	encodeJson := base64.StdEncoding.EncodeToString(ebJson)
	//构造push对象
	var fTypes []falconType
	fType := &falconType{
		Endpoint:    "k8sEvent",
		Metric:      "event",
		Timestamp:   time.Now().UTC().Unix(),
		Step:        1,
		Value:       encodeJson,
		CounterType: "GAUGE",
		Tags:        "",
	}
	fTypes = append(fTypes, *fType)
	fTypeJson, err := json.Marshal(fTypes)
	if err != nil {
		glog.Errorf("marshal falcon type to json error:%s", err)
	}
	this.concurrentSendEvent(fTypeJson)
}

//并发发送数据
func (sink *openfalconSink) concurrentSendEvent(fTypeJson []byte) {
	glog.Infof("并发发送数据")
	sink.wg.Add(1)
	//带缓存的channel，当达到最大的并发请求的时候阻塞
	//将匿名孔结构体放入channel中
	sink.conChan <- struct{}{}
	go func(fTypeJson []byte) {
		sink.sendEvent(fTypeJson)
	}(fTypeJson)
}

func (this *openfalconSink) sendEvent(fTypeJson []byte) {
	defer func() {
		<-this.conChan
	}()
	glog.Infof("发送数据ing:%s", fTypeJson)
	//http发送 todo 尝试3次
	resp, err := http.Post(this.host, "application/json", bytes.NewReader(fTypeJson))
	if err != nil {
		glog.Errorf("send event to falcon error:%s", err)
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("read response body error:%s", err)
	}
	glog.V(4).Infof("response body %s", respBody)
}

func CreateFalconSink(uri *url.URL) (*openfalconSink, error) {
	falconSink := &openfalconSink{
		host:    uri.Scheme + "://" + uri.Host + uri.Path,
		conChan: make(chan struct{}, 10),
	}
	glog.Infof("created openfalcon sink with options: host:%s", uri.Scheme+"://"+uri.Host+uri.Path)
	return falconSink, nil
}
