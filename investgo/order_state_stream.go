package investgo

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/jabobon1/invest-api-go-sdk/proto"
)

type OrderStateStream struct {
	stream       pb.OrdersStreamService_OrderStateStreamClient
	ordersClient *OrdersStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	states chan *pb.OrderStateStreamResponse_OrderState
	pings  chan *pb.Ping
}

// OrderState - Метод возвращает канал для чтения информации о состоянии поручений
func (s *OrderStateStream) OrderState() <-chan *pb.OrderStateStreamResponse_OrderState {
	return s.states
}

func (s *OrderStateStream) Pings() <-chan *pb.Ping {
	return s.pings
}

// Listen - метод начинает слушать стрим и отправлять информацию в канал, для получения канала: OrderState()
func (s *OrderStateStream) Listen() error {
	defer s.shutdown()
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			resp, err := s.stream.Recv()
			if err != nil {
				switch {
				case status.Code(err) == codes.Canceled:
					s.ordersClient.logger.Infof("stop listening order state stream resp: %s err:%v", resp.String(), err)
					return nil
				default:
					return err
				}
			} else {
				switch resp.GetPayload().(type) {
				case *pb.OrderStateStreamResponse_OrderState_:
					s.states <- resp.GetOrderState()
					select {
					case s.pings <- resp.GetPing():
					default:
					}
				case *pb.OrderStateStreamResponse_Ping:
					p := resp.GetPing()
					if p != nil {
						s.ordersClient.logger.Infof("[OrderStream] Ping received time:%v streamID:%s", p.Time, p.StreamId)
					} else {
						s.ordersClient.logger.Infof("[OrderStream] Ping received")
					}
					select {
					case s.pings <- p:
					default:
					}
				default:
					s.ordersClient.logger.Infof("info from order state stream %v", resp.String())
				}
			}
		}
	}
}

func (s *OrderStateStream) restart(_ context.Context, attempt uint, err error) {
	s.ordersClient.logger.Infof("try to restart order state stream err = %v, attempt = %v", err.Error(), attempt)
}

func (s *OrderStateStream) shutdown() {
	s.ordersClient.logger.Infof("close order state stream")
	close(s.states)
	close(s.pings)
}

// Stop - Завершение работы стрима
func (s *OrderStateStream) Stop() {
	s.cancel()
}
