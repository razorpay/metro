package publisher

import (
	"context"
	"testing"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestPublishValidation_ZeroMessages(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic: "projects/project-001/topics/topic-001",
	}

	e := ValidatePublishRequest(ctx, req)
	assert.Equal(t, messagesEmptyError, e)
}

func TestPublishValidation_MultipleOrderingKeys(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic: "projects/project-001/topics/topic-001",
		Messages: []*metrov1.PubsubMessage{
			{
				Data:        []byte("d1"),
				OrderingKey: "o1",
			},
			{
				Data:        []byte("d2"),
				OrderingKey: "o2",
			},
		},
	}

	e := ValidatePublishRequest(ctx, req)
	assert.Equal(t, multipleOrderingKeysError, e)
}

func TestPublishValidation_InvalidOrderingKeyLength(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic: "projects/project-001/topics/topic-001",
		Messages: []*metrov1.PubsubMessage{
			{
				Data:        []byte("d1"),
				OrderingKey: `kWo:.X&H&7DAR^YEff,OO?J!Q'41'm:mAP2Ac&).S^ApmHxJ>6A2bRXf<w1.<!;uRAsh7sx2XYTUxMp^Xtnm5:m<p.bLzX.0ic7(tzp$p<V4,T/!*ZClho$VJfqfrZ'ucHh)&&EY?Ls/*5Dla;^aomMZb7N.Y;$YGpP6!1>VMb6p*n3,kVUjfeuLX@nNRv519iL@OrKIYkV*)ZM@PZ1:j^liZ1y8b*p8IBbfP$ES1kjA4v3n6Y?@kX^dFo(5YQYnds@j.HL9b6&YbFmEksRzUamerIbr4.73At>R/S4M'Sd04sbJoZ.(7)7)wBV:2Z#OJQkyql'O@<bJGWpdHtQ.EyBAB0rwP2*Qe!LOwSQes2z2Zk4ld5^!w!*rm>9P@MCPG*'($5aJKrG&jZ4Af9d'VC2(K)mO@M)foG:p9v,y(8NL8YSj;ePqco15aA<wc9V^K:oU3fLdGLp/DX@h#2t>y#VJpo*jN5@3z>XyA&sz/m:nj@f71Vp@:'ihwaMm(yfA1YI$tp5#6LobDBvN>e*v'jKdKUVd1ThUvlb7@vbbUF<580Mu&;q8jbn^/SsFYNu/Jdp9bak!$.(*I1J:E#cO54VA('sCMhz$T6pnU2y^dYaKha/3#g.$kt:^;Gmy6ZW>w>MTFAf^yZouDD4VC4!n7?YhFFK5LU2^*KjFQKxh,xL7RomMC?EE2sH?mhOm8<sqBM?r:f:9httxXu7tl6!D?.Yc^Otk,$2e,chlfFRgm9PaFFw1iTr6WS,hd>efnifZnl&b7LO/5*5aWYIaBtkI?mblGAErUQovlxk!w'Jdh,LpzCy,Cxy1,V(fhs;RzyaoHabNwg#4p2Pr,v(o6oWSldB@.kqC(*xy$8)o4H^1zcO/JgkANnCr.CNiB*.QRR38NMxvdwdFyhwCrF8)/R4PJUc7Wk.7apkJAk1B&s.F.'1O5gqyk*HPz?YprfVxwXU0)QQe9G&oy.o>kT)nxk6F9XLDF#VtK,trCD>::uc0MjuS<!59o3I5n8SsOow7w?uZW`,
			},
		},
	}

	e := ValidatePublishRequest(ctx, req)
	assert.Equal(t, orderingKeyTooLongError, e)
}

func TestPublishValidation_Success(t *testing.T) {
	ctx := context.Background()
	req := &metrov1.PublishRequest{
		Topic: "projects/project-001/topics/topic-001",
		Messages: []*metrov1.PubsubMessage{
			{
				Data:        []byte("d1"),
				OrderingKey: `o`,
			},
			{
				Data:        []byte("d1"),
				OrderingKey: `o`,
			},
		},
	}

	e := ValidatePublishRequest(ctx, req)
	assert.Equal(t, nil, e)
}
