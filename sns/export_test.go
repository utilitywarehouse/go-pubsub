package sns

import "github.com/aws/aws-sdk-go/service/sns/snsiface"

func NewTestSNSSink(snsapi snsiface.SNSAPI, topic string) *SNSSink {
	return &SNSSink{
		client: snsapi,
		topic:  topic,
	}
}
