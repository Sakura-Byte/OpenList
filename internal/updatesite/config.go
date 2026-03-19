package updatesite

type DeliveryConfig struct {
	Enabled      bool
	InstanceName string
	WebhookURL   string
	Secret       string
	BaseURL      string
}

var configProvider func() DeliveryConfig

func SetConfigProvider(provider func() DeliveryConfig) {
	configProvider = provider
}

func currentConfig() DeliveryConfig {
	if configProvider == nil {
		return DeliveryConfig{}
	}
	return configProvider()
}
