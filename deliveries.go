package rmq

type Deliveries []Delivery

func (deliveries Deliveries) Ack() (count int, err error) {
	failedCount := 0
	for _, delivery := range deliveries {
		ok, err := delivery.Ack()
		if err != nil {
			return failedCount, err
		}
		if !ok {
			failedCount++
		}
	}
	return failedCount, nil
}

func (deliveries Deliveries) Reject() (count int, err error) {
	failedCount := 0
	for _, delivery := range deliveries {
		ok, err := delivery.Reject()
		if err != nil {
			return failedCount, err
		}
		if !ok {
			failedCount++
		}
	}
	return failedCount, nil
}
