module github.com/razorpay/metro

go 1.16

require (
	cloud.google.com/go v0.84.0 // indirect
	cloud.google.com/go/pubsub v1.11.0
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/apache/pulsar-client-go v0.5.0
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20210615012709-cb72395fb53f // indirect
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/danieljoos/wincred v1.1.0 // indirect
	github.com/dvsekhvalnov/jose2go v0.0.0-20201001154944-b09cfaf05951 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/getsentry/sentry-go v0.11.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.2.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.5.0
	github.com/gtank/cryptopasta v0.0.0-20170601214702-1f550f6f2f69
	github.com/hashicorp/consul/api v1.8.1
	github.com/hashicorp/go-hclog v0.15.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/keybase/go-keychain v0.0.0-20201121013009-976c83ec27a6 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/kris-nova/lolgopher v0.0.0-20210112022122-73f0047e8b65 // indirect
	github.com/magiconair/properties v1.8.5
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/olekukonko/tablewriter v0.0.4 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/rakyll/statik v0.1.7
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rs/xid v1.3.0
	github.com/sethvargo/go-password v0.2.0
	github.com/spf13/cobra v1.1.3 // indirect
	github.com/spf13/viper v1.8.0
	github.com/streamnative/pulsarctl v0.4.3-0.20210116042116-1fe5d713915c
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	go.uber.org/atomic v1.8.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.17.0
	golang.org/x/crypto v0.0.0-20201221181555-eec23a3978ad // indirect
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/term v0.0.0-20210615171337-6886f2dfbf5b // indirect
	golang.org/x/tools v0.1.4 // indirect
	google.golang.org/api v0.48.0
	google.golang.org/genproto v0.0.0-20210617175327-b9e0b3197ced
	google.golang.org/grpc v1.39.0-dev.0.20210519181852-3dd75a6888ce
	google.golang.org/protobuf v1.27.1
)

// https://github.com/99designs/keyring/issues/64#issuecomment-742903794
replace github.com/keybase/go-keychain => github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4
