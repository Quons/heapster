package openfalcon

import (
	"bytes"
	"encoding/json"
	"github.com/golang/glog"
	"io/ioutil"
	"k8s.io/heapster/metrics/core"
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
	Endpoint    string  `json:"endpoint"`
	Metric      string  `json:"metric"`
	Timestamp   int64   `json:"timestamp"`
	Step        int     `json:"step"`
	Value       float64 `json:"value"`
	CounterType string  `json:"counterType"`
	Tags        string  `json:"tags"`
}

const (
	// 单次最大发送数目
	maxSendBatchSize = 5
)

func (sink *openfalconSink) Name() string {
	return "openfalcon Sink"
}

func (sink *openfalconSink) ExportData(dataBatch *core.DataBatch) {
	glog.Infof("start export data")
	dbJson, err := json.Marshal(dataBatch)
	if err != nil {
		glog.Infof("dataBatch to json err:%s", err)
		return
	}
	glog.V(2).Infof("dataBatch json:%s", dbJson)
	sink.Lock()
	defer sink.Unlock()
	var endpoint string
	fts := make([]falconType, 0, 0)
	for metricSetName, metricSet := range dataBatch.MetricSets {
		//这里可以添加过滤
		labelType := metricSet.Labels["type"]
		switch labelType {
		case "sys_container":
			continue
		default:
			endpoint = metricSetName
		}
		for metricName, metricValue := range metricSet.MetricValues {
			//转换数据类型，都变成float64
			var value float64
			if core.ValueInt64 == metricValue.ValueType {
				value = float64(metricValue.IntValue)
			} else if core.ValueFloat == metricValue.ValueType {
				value = float64(metricValue.FloatValue)
			} else {
				continue
			}
			ft := falconType{
				Metric:      metricName,
				Endpoint:    endpoint,
				Timestamp:   metricSet.ScrapeTime.UTC().Unix(),
				Step:        10,
				Value:       value,
				CounterType: "GAUGE",
				Tags:        "",
			}
			fts = append(fts, ft)
			if len(fts) >= maxSendBatchSize {
				sink.concurrentSendData(fts)
				//清空fts
				fts = make([]falconType, 0, 0)
			}
		}
		//处理labeledMetric
		for _, labeledMetric := range metricSet.LabeledMetrics {
			var value float64
			if core.ValueInt64 == labeledMetric.ValueType {
				value = float64(labeledMetric.IntValue)
			} else if core.ValueFloat == labeledMetric.ValueType {
				value = float64(labeledMetric.FloatValue)
			} else {
				continue
			}
			ft := falconType{
				Endpoint:    metricSetName,
				Metric:      labeledMetric.Name,
				Timestamp:   metricSet.ScrapeTime.UTC().Unix(),
				Step:        10,
				Value:       value,
				CounterType: "GAUGE",
				Tags:        labeledMetric.Labels["resource_id"],
			}
			fts = append(fts, ft)
			if len(fts) >= maxSendBatchSize {
				sink.concurrentSendData(fts)
				fts = make([]falconType, 0, 0)
			}
		}
	}
	if len(fts) >= 0 {
		//发送最后的数据
		sink.concurrentSendData(fts)
	}
	sink.wg.Wait()
}

//并发发送数据
func (sink *openfalconSink) concurrentSendData(fts []falconType) {
	sink.wg.Add(1)
	//带缓存的channel，当达到最大的并发请求的时候阻塞
	//将匿名孔结构体放入channel中
	sink.conChan <- struct{}{}
	go func(fts []falconType) {
		sink.sendData(fts)
	}(fts)
}

//发送数据
func (sink *openfalconSink) sendData(fts []falconType) {
	defer func() {
		// empty an item from the channel so the next waiting request can run
		<-sink.conChan
		sink.wg.Done()
	}()

	falconJson, err := json.Marshal(fts)
	if err != nil {
		glog.V(2).Infof("Error: %v ,fail to marshal event to falcon type", err)
		return
	}

	glog.V(4).Infof("push json info %s", falconJson)
	resp, err := http.Post(sink.host, "application/json", bytes.NewReader(falconJson))
	if err != nil {
		glog.V(2).Infof("Error: %v ,fail to send %s to falcon ,err %s", string(falconJson[:]), err)
	}
	defer resp.Body.Close()
	s, _ := ioutil.ReadAll(resp.Body)
	glog.V(4).Infof("openfalcon response body :%s", s)
	start := time.Now()
	end := time.Now()
	glog.V(4).Infof("Exported %d data to falcon in %s", len(fts), end.Sub(start))
}

func (sink *openfalconSink) Stop() {
}

func CreateFalconSink(uri *url.URL) (core.DataSink, error) {
	//返回falconSink对象,封装uri参数，falcon上报的地址
	sink := &openfalconSink{
		host: uri.Scheme + "://" + uri.Host + uri.Path,
		//设置最大的并发请求量
		conChan: make(chan struct{}, 10),
	}
	glog.Infof("created openfalcon sink with options: host:%s", uri.Host)
	return sink, nil
}
