package eino

import (
	"context"
	"errors"
	"fmt"

	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/adapters/analyzer/rule"
	"github.com/charlesAcmen/livestream-danmaku/v11/insight/internal/domain"
	"github.com/cloudwego/eino/compose"
)

type Analyzer struct {
	runnable compose.Runnable[domain.InsightWindow, domain.AnalysisResult]
}

type preparedInsight struct {
	window  domain.InsightWindow
	rules   domain.RuleStats
	request CompletionRequest
}

type completedInsight struct {
	preparedInsight
	response CompletionResponse
}

func NewAnalyzer(model CompletionModel) (*Analyzer, error) {
	if model == nil {
		return nil, errors.New("completion model is required")
	}

	graph := compose.NewGraph[domain.InsightWindow, domain.AnalysisResult]()
	if err := graph.AddLambdaNode("prepare", compose.InvokableLambda(func(ctx context.Context, window domain.InsightWindow) (preparedInsight, error) {
		if err := ctx.Err(); err != nil {
			return preparedInsight{}, err
		}
		rules, err := rule.NewAnalyzer().Analyze(ctx, window)
		if err != nil {
			return preparedInsight{}, err
		}
		return preparedInsight{window: window, rules: rules.Rules, request: buildCompletionRequest(window)}, nil
	})); err != nil {
		return nil, fmt.Errorf("add prepare node: %w", err)
	}
	if err := graph.AddLambdaNode("complete", compose.InvokableLambda(func(ctx context.Context, prepared preparedInsight) (completedInsight, error) {
		if err := ctx.Err(); err != nil {
			return completedInsight{}, err
		}
		response, err := model.Complete(ctx, prepared.request)
		if err != nil {
			return completedInsight{}, err
		}
		return completedInsight{preparedInsight: prepared, response: response}, nil
	})); err != nil {
		return nil, fmt.Errorf("add complete node: %w", err)
	}
	if err := graph.AddLambdaNode("parse_and_validate", compose.InvokableLambda(func(ctx context.Context, completed completedInsight) (domain.AnalysisResult, error) {
		if err := ctx.Err(); err != nil {
			return domain.AnalysisResult{}, err
		}
		semantic, err := parseAndValidate(completed.response.Content, completed.window)
		if err != nil {
			return domain.AnalysisResult{}, err
		}
		return domain.AnalysisResult{
			Rules: completed.rules, Semantic: semantic,
			Model: domain.ModelMeta{
				Provider: completed.response.Provider, Model: completed.response.Model, PromptVersion: promptVersion,
				InputTokens: completed.response.InputTokens, OutputTokens: completed.response.OutputTokens,
				LatencyMillis: completed.response.Latency.Milliseconds(),
			},
		}, nil
	})); err != nil {
		return nil, fmt.Errorf("add parse_and_validate node: %w", err)
	}
	for _, edge := range [][2]string{{compose.START, "prepare"}, {"prepare", "complete"}, {"complete", "parse_and_validate"}, {"parse_and_validate", compose.END}} {
		if err := graph.AddEdge(edge[0], edge[1]); err != nil {
			return nil, fmt.Errorf("add graph edge %s -> %s: %w", edge[0], edge[1], err)
		}
	}

	runnable, err := graph.Compile(context.Background())
	if err != nil {
		return nil, fmt.Errorf("compile insight graph: %w", err)
	}
	return &Analyzer{runnable: runnable}, nil
}

func (a *Analyzer) Analyze(ctx context.Context, window domain.InsightWindow) (domain.AnalysisResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.AnalysisResult{}, err
	}
	return a.runnable.Invoke(ctx, window)
}
