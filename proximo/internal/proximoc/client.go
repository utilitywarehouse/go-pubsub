package proximoc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"log"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func ConsumeContext(ctx context.Context, proximoAddress string, consumer string, topic string, f func(*Message) error) error {
	return consumeContext(ctx, proximoAddress, consumer, topic, f, grpc.WithInsecure())
}

func consumeContext(ctx context.Context, proximoAddress string, consumer string, topic string, f func(*Message) error, opts ...grpc.DialOption) error {

	var wg sync.WaitGroup
	defer wg.Wait()

	conn, err := grpc.DialContext(ctx, proximoAddress, opts...)
	if err != nil {
		return errors.Wrapf(err, "fail to dial %s", proximoAddress)
	}
	defer conn.Close()
	client := NewMessageSourceClient(conn)

	stream, err := client.Consume(ctx)
	if err != nil {
		return errors.Wrap(err, "fail to consume")
	}

	defer stream.CloseSend()

	handled := make(chan string)
	errs := make(chan error, 2)

	ins := make(chan *Message, 16) // TODO: make buffer size configurable?

	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ins)
		for {
			in, err := stream.Recv()
			if err != nil {
				if err != io.EOF && grpc.Code(err) != 1 { // 1 means cancelled
					errs <- err
				}
				return
			}
			select {
			case ins <- in:
			case <-localCtx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case in, ok := <-ins:
				if !ok {
					return
				}
				if err := f(in); err != nil {
					errs <- err
					return
				}
				select {
				case handled <- in.GetId():
				case <-localCtx.Done():
					return
				}
			case <-localCtx.Done():
				return
			}
		}
	}()

	if err := stream.Send(&ConsumerRequest{
		StartRequest: &StartConsumeRequest{
			Topic:    topic,
			Consumer: consumer,
		},
	}); err != nil {
		return err
	}

	for {
		select {
		case id := <-handled:
			if err := stream.Send(&ConsumerRequest{Confirmation: &Confirmation{MsgID: id}}); err != nil {
				if grpc.Code(err) == 1 {
					return nil
				}
				return err
			}
		case err := <-errs:
			return err
		case <-localCtx.Done():
			return nil // ctx.Err()
		}

	}

}

func DialProducer(ctx context.Context, proximoAddress string, topic string) (*ProducerConn, error) {
	return dialProducer(ctx, proximoAddress, topic, grpc.WithInsecure())
}

func dialProducer(ctx context.Context, proximoAddress string, topic string, opts ...grpc.DialOption) (*ProducerConn, error) {

	conn, err := grpc.DialContext(ctx, proximoAddress, opts...)
	if err != nil {
		return nil, err
	}

	client := NewMessageSinkClient(conn)

	stream, err := client.Publish(ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}

	if err := stream.Send(&PublisherRequest{
		StartRequest: &StartPublishRequest{
			Topic: topic,
		},
	}); err != nil {
		conn.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	pc := &ProducerConn{conn, ctx, cancel, stream, make(chan req), make(chan error), sync.WaitGroup{}}
	pc.start()
	return pc, nil
}

type req struct {
	data []byte
	resp chan error
}

type ProducerConn struct {
	cc *grpc.ClientConn

	ctx    context.Context
	cancel func()

	stream MessageSink_PublishClient

	reqs chan req

	errs chan error

	wg sync.WaitGroup
}

func (p *ProducerConn) Produce(message []byte) error {
	err := make(chan error)
	r := req{message, err}
	select {
	case p.reqs <- r:
	case e := <-p.errs:
		return e
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	var e error
	select {
	case e = <-err:
	case e = <-p.errs:
	case <-p.ctx.Done():
	}
	return e
}

func (p *ProducerConn) Close() error {
	p.cancel()
	err := p.cc.Close()
	p.wg.Wait()
	return err
}

func (p *ProducerConn) start() error {

	// 	defer p.stream.CloseSend()

	confirmations := make(chan *Confirmation, 16) // TODO: make buffer size configurable?

	recvErr := make(chan error, 1)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			conf, err := p.stream.Recv()
			if err != nil {
				if err != io.EOF && grpc.Code(err) != 1 { // 1 means cancelled
					recvErr <- err
				}
				return
			}
			confirmations <- conf
		}
	}()

	idErr := make(map[string]chan error)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var lerr error

	mainLoop:
		for {
			select {
			case err := <-recvErr:
				lerr = err
				break mainLoop
			case in := <-confirmations:
				ec := idErr[in.GetMsgID()]
				if ec == nil {
					lerr = errUnexpectedMessageId
					break mainLoop
				}
				ec <- nil
				delete(idErr, in.GetMsgID())
			case req := <-p.reqs:
				id := makeId()
				idErr[id] = req.resp
				if err := p.stream.Send(&PublisherRequest{Msg: &Message{Data: req.data, Id: id}}); err != nil {
					if grpc.Code(err) != 1 {
						lerr = err
						log.Printf("err error %v\n", err)
					}
					break mainLoop
				}
			case <-p.ctx.Done():
				break mainLoop
			}
		}

		var errs chan error
		if lerr != nil {
			errs = p.errs
		}

	errLoop:
		for {
			select {
			case errs <- lerr:
			case <-p.ctx.Done():
				break errLoop
			}
		}
	}()

	return nil
}

var (
	errUnexpectedMessageId = errors.New("unexpected message id")
)

func makeId() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(random)
}

// GRPCClient returns grpc client connection
func (p *ProducerConn) GRPCClient() *grpc.ClientConn {
	return p.cc
}
