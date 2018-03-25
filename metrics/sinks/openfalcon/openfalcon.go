package openfalcon

import (
	"encoding/json"
	"github.com/golang/glog"
	"k8s.io/heapster/metrics/core"
	"net/url"
	"strings"
	"sync"
	"time"
)

type openfalconSink struct {
	//这里类似于继承RWMutex
	sync.RWMutex
	host string
	// wg and conChan will work together to limit concurrent influxDB sink goroutines.
	wg sync.WaitGroup
	//使用到了空结构体最为chan存储类型
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
	sink.Lock()
	defer sink.Unlock()

	var endpoint string
	fts := make([]falconType, 0, 0)
	glog.Infof("dataBatch %v", dataBatch.MetricSets)
	for _, metricSet := range dataBatch.MetricSets {
		//遍历metricSet.label 获取endpoint信息
		for labelName, labelValue := range metricSet.Labels {
			//获取nodename 或者hostname
			glog.V(4).Infof("labelName:%s  labeVlue: %s", labelName, labelValue)
			if strings.EqualFold(labelName, "nodename") {
				endpoint = labelValue
			}
			//metricSet.Labels key:nodename   value:k8s-node-1
			//metricSet.Labels key:hostname   value:k8s-node-1
			//metricSet.Labels key:host_id   value:k8s-node-1
			//metricSet.Labels key:type   value:sys_container
			//metricSet.Labels key:container_name   value:system.slice/chronyd.service
		}
		glog.V(4).Infof("metricvalues: %v", metricSet.MetricValues)
		for metricName, metricValue := range metricSet.MetricValues {
			glog.V(4).Infof("metricName:%s  metricValue:%s", metricName, metricValue)
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
				Step:        5,
				Value:       value,
				CounterType: "GAUGE",
				Tags:        "cluster=,label=",
			}
			glog.V(4).Infof("raw metricValues：%s", ft)
			fts = append(fts, ft)
			glog.Infof("fts :%v", fts)
			glog.Infof("start send ......................")
			if len(fts) >= maxSendBatchSize {
				//推送
				sink.concurrentSendData(fts)
				//清空fts
				fts = make([]falconType, 0, 0)
			}
		}
		//处理labelMetric
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
				Endpoint:    endpoint,
				Metric:      labeledMetric.Name,
				Timestamp:   metricSet.ScrapeTime.UTC().Unix(),
				Step:        5,
				Value:       value,
				CounterType: "GAUGE",
				Tags:        "cluster=,label=",
			}
			glog.V(4).Infof("raw labelMetric falconType:%v", ft)
			//{k8s-node-1 disk/io_write_bytes 1521779865 5 0 GAUGE cluster=,label=}
			//{k8s-node-1 disk/io_read_bytes_rate 1521779865 5 0 GAUGE cluster=,label=}
			for key, value := range labeledMetric.Labels {
				glog.V(4).Infof("[labeledMetric]   labeledMetric.Labels  key:%v  value :%v", key, value)
				//labeledMetric.Labels  key:resource_id  value :253:0
			}
			glog.V(4).Infof("[labeledMetric]  start send labelMetric ..........")
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

//同步发送数据
func (sink *openfalconSink) concurrentSendData(fts []falconType) {
	glog.V(4).Infof("start concurrentsend .................")
	sink.wg.Add(1)
	// 带缓存的channel，当达到最大的并发请求的时候阻塞
	//将匿名孔结构体放入channel中
	sink.conChan <- struct{}{}
	go func(fts []falconType) {
		sink.sendData(fts)
	}(fts)
}

//发送数据
func (sink *openfalconSink) sendData(fts []falconType) {
	glog.V(4).Infof("start send final ....................")
	defer func() {
		// empty an item from the channel so the next waiting request can run
		<-sink.conChan
		sink.wg.Done()
	}()

	//falconType转换成json
	falconJson, err := json.Marshal(fts)
	if err != nil {
		glog.V(2).Infof("Error: %v ,fail to marshal event to falcon type", err)
		return
	}

	glog.V(3).Infof("push json info %s", falconJson)
	/*if resp, err := http.Post("url", "application/json", bytes.NewReader(falconJson)); err != nil {
		defer resp.Body.Close()
		s, _ := ioutil.ReadAll(resp.Body)
		glog.V(2).Infof("Error: %v ,fail to send %s to falcon ,err %s", s, err)
		return
	}*/
	start := time.Now()
	end := time.Now()
	glog.V(4).Infof("Exported %d data to falcon in %s", len(fts), end.Sub(start))
}

func (sink *openfalconSink) Stop() {
}

func CreateFalconSink(uri *url.URL) (core.DataSink, error) {
	//返回falconSink对象,封装uri参数，falcon上报的地址
	sink := &openfalconSink{
		host: uri.Host,
		//设置最大的并发请求量
		conChan: make(chan struct{}, 10),
	}
	glog.Infof("created openfalcon sink with options: host:%s", uri.Host)
	return sink, nil
}
