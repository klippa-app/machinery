package tracing_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/tracing"

	opentracing "github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartSpanFromHeaders_NoExistingSpan(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracing.StartSpanFromHeaders(tasks.Headers{}, "test_op")
	require.NotNil(t, span)
	span.Finish()

	spans := tracer.FinishedSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "test_op", spans[0].OperationName)
	assert.Equal(t, "machinery", spans[0].Tag(string(tracing.MachineryTag.Key)))
}

func TestStartSpanFromHeaders_WithExistingSpan(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	parentSpan := tracer.StartSpan("parent_op")
	headers := tracing.HeadersWithSpan(tasks.Headers{}, parentSpan)

	span := tracing.StartSpanFromHeaders(headers, "child_op")
	require.NotNil(t, span)
	span.Finish()
	parentSpan.Finish()

	spans := tracer.FinishedSpans()
	require.Len(t, spans, 2)

	var child *mocktracer.MockSpan
	for _, s := range spans {
		if s.OperationName == "child_op" {
			child = s
		}
	}
	require.NotNil(t, child)
	assert.Equal(t, parentSpan.(*mocktracer.MockSpan).SpanContext.SpanID, child.ParentID)
}

type failingInjectTracer struct {
	opentracing.Tracer
}

func (f failingInjectTracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	return opentracing.ErrInvalidCarrier
}

func TestHeadersWithSpan_InjectError(t *testing.T) {
	inner := mocktracer.New()
	span := inner.StartSpan("op")

	opentracing.SetGlobalTracer(failingInjectTracer{inner})
	defer opentracing.SetGlobalTracer(inner)

	headers := tracing.HeadersWithSpan(tasks.Headers{}, span)
	require.NotNil(t, headers)

	span.Finish()
	spans := inner.FinishedSpans()
	require.Len(t, spans, 1)
	logs := spans[0].Logs()
	require.NotEmpty(t, logs)
}

func TestHeadersWithSpan_NilHeaders(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")
	headers := tracing.HeadersWithSpan(nil, span)

	require.NotNil(t, headers)
	assert.NotEmpty(t, headers)
}

func TestHeadersWithSpan_ExistingHeaders(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")
	headers := tracing.HeadersWithSpan(tasks.Headers{"foo": "bar"}, span)

	require.NotNil(t, headers)
	assert.Equal(t, "bar", headers["foo"])
}

func TestConsumerOption(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	producerSpan := tracer.StartSpan("producer_op")
	producerCtx := producerSpan.Context()

	opt := tracing.ConsumerOption(producerCtx)
	require.NotNil(t, opt)

	span := opentracing.StartSpan("consumer_op", opt)
	span.Finish()
	producerSpan.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, opentracing_ext.SpanKindConsumerEnum, mockSpan.Tag("span.kind"))
}

func TestConsumerOption_NilContext(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	opt := tracing.ConsumerOption(nil)
	span := opentracing.StartSpan("consumer_op", opt)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, opentracing_ext.SpanKindConsumerEnum, mockSpan.Tag("span.kind"))
}

func TestProducerOption(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	opt := tracing.ProducerOption()
	span := opentracing.StartSpan("producer_op", opt)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, opentracing_ext.SpanKindProducerEnum, mockSpan.Tag("span.kind"))
}

func TestAnnotateSpanWithSignatureInfo(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")

	signature := &tasks.Signature{
		Name: "test_task",
		UUID: "task-uuid",
	}
	tracing.AnnotateSpanWithSignatureInfo(span, signature)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, "test_task", mockSpan.Tag("signature.name"))
	assert.Equal(t, "task-uuid", mockSpan.Tag("signature.uuid"))
	assert.Nil(t, mockSpan.Tag("signature.group.uuid"))
	assert.Nil(t, mockSpan.Tag("signature.chord.callback.uuid"))
}

func TestAnnotateSpanWithSignatureInfo_WithGroupAndChordCallback(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")

	signature := &tasks.Signature{
		Name:      "test_task",
		UUID:      "task-uuid",
		GroupUUID: "group-uuid",
		ChordCallback: &tasks.Signature{
			UUID: "callback-uuid",
			Name: "callback_task",
		},
	}
	tracing.AnnotateSpanWithSignatureInfo(span, signature)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, "group-uuid", mockSpan.Tag("signature.group.uuid"))
	assert.Equal(t, "callback-uuid", mockSpan.Tag("signature.chord.callback.uuid"))
	assert.Equal(t, "callback_task", mockSpan.Tag("signature.chord.callback.name"))
}

func TestAnnotateSpanWithChainInfo(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")

	chain := &tasks.Chain{
		Tasks: []*tasks.Signature{
			{UUID: "1"},
			{UUID: "2"},
		},
	}
	tracing.AnnotateSpanWithChainInfo(span, chain)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, 2, mockSpan.Tag("chain.tasks.length"))

	for _, signature := range chain.Tasks {
		assert.NotEmpty(t, signature.Headers)
	}
}

func TestAnnotateSpanWithGroupInfo(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")

	group := &tasks.Group{
		GroupUUID: "group-uuid",
		Tasks: []*tasks.Signature{
			{UUID: "1"},
			{UUID: "2"},
			{UUID: "3"},
		},
	}
	tracing.AnnotateSpanWithGroupInfo(span, group, 5)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, "group-uuid", mockSpan.Tag("group.uuid"))
	assert.Equal(t, 3, mockSpan.Tag("group.tasks.length"))
	assert.Equal(t, 5, mockSpan.Tag("group.concurrency"))
	assert.Equal(t, `["1","2","3"]`, mockSpan.Tag("group.tasks"))

	for _, signature := range group.Tasks {
		assert.NotEmpty(t, signature.Headers)
	}
}

func TestAnnotateSpanWithChordInfo(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("op")

	chord := &tasks.Chord{
		Group: &tasks.Group{
			GroupUUID: "group-uuid",
			Tasks: []*tasks.Signature{
				{UUID: "1"},
			},
		},
		Callback: &tasks.Signature{UUID: "callback-uuid"},
	}
	tracing.AnnotateSpanWithChordInfo(span, chord, 2)
	span.Finish()

	mockSpan := span.(*mocktracer.MockSpan)
	assert.Equal(t, "callback-uuid", mockSpan.Tag("chord.callback.uuid"))
	assert.NotEmpty(t, chord.Callback.Headers)
	assert.Equal(t, "group-uuid", mockSpan.Tag("group.uuid"))
}
