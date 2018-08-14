package sns

import "github.com/aws/aws-sdk-go/service/sns/snsiface"

func NewTestSNSSink(snsapi snsiface.SNSAPI, topic string) *messageSink {
	return &messageSink{
		client: snsapi,
		topic:  topic,
	}
}
