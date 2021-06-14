module github.com/razorpay/metro

go 1.16

require (
	cloud.google.com/go/pubsub v1.9.0
	github.com/apache/pulsar-client-go v0.3.0
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20210115012126-2456729a54ca // indirect
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/confluentinc/confluent-kafka-go v1.6.1
	github.com/danieljoos/wincred v1.1.0 // indirect
	github.com/dvsekhvalnov/jose2go v0.0.0-20201001154944-b09cfaf05951 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/getsentry/sentry-go v0.8.0
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.4-0.20210303013846-acacf8158c9a
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.0.1
	github.com/hashicorp/consul/api v1.8.1
	github.com/hashicorp/go-hclog v0.15.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/keybase/go-keychain v0.0.0-20201121013009-976c83ec27a6 // indirect
	github.com/klauspost/compress v1.11.13 // indirect
	github.com/kris-nova/lolgopher v0.0.0-20210112022122-73f0047e8b65 // indirect
	github.com/magiconair/properties v1.8.4
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/olekukonko/tablewriter v0.0.4 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/rakyll/statik v0.1.7
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rs/xid v1.2.1
	github.com/sethvargo/go-password v0.2.0
	github.com/spf13/afero v1.2.2 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v1.1.3 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/viper v1.7.1
	github.com/streamnative/pulsarctl v0.4.3-0.20210116042116-1fe5d713915c
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.24.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201221181555-eec23a3978ad // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/oauth2 v0.0.0-20210113205817-d3ed898aa8a3 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/term v0.0.0-20201210144234-2321bbc49cbf // indirect
	golang.org/x/text v0.3.6 // indirect
	golang.org/x/tools v0.1.2 // indirect
	google.golang.org/api v0.36.0
	google.golang.org/genproto v0.0.0-20210315173758-2651cd453018
	google.golang.org/grpc v1.37.0-dev.0.20210309003715-fce74a94bdff
	google.golang.org/protobuf v1.25.1-0.20210303022638-839ce436895b
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

// https://github.com/99designs/keyring/issues/64#issuecomment-742903794
replace github.com/keybase/go-keychain => github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4
