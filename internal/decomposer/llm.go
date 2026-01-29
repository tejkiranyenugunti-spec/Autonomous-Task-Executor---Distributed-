package decomposer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/autonomous-ate/engine/internal/models"
	"github.com/google/generative-ai-go/genai"
	"google.golang.org/api/option"
)

const llmSystemPrompt = `You are a task decomposition engine for an AI agent 
execution system. Given a high-level instruction, break 
it into a JSON array of ordered subtasks. Each subtask 
must have: id (string), name (string), description 
(string), order (int), estimated_duration_seconds (int). 
Return ONLY valid JSON, no explanation, no markdown.`

// Subtask is the structured unit returned by the LLM planner before conversion
// to wire messages for Kafka/RabbitMQ.
type Subtask struct {
	ID                       string `json:"id"`
	Name                     string `json:"name"`
	Description              string `json:"description"`
	Order                    int    `json:"order"`
	EstimatedDurationSeconds int    `json:"estimated_duration_seconds"`
}

// planWithGemini calls Google Gemini and parses a JSON array of Subtask.
// On any failure it returns a non-nil error; callers should fall back to manual decomposition.
func planWithGemini(ctx context.Context, apiKey, instruction string) ([]Subtask, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, errors.New("empty API key")
	}
	if strings.TrimSpace(instruction) == "" {
		return nil, errors.New("empty instruction")
	}

	client, err := genai.NewClient(ctx, option.WithAPIKey(apiKey))
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close() }()

	model := client.GenerativeModel("gemini-1.5-flash")
	model.ResponseMIMEType = "application/json"

	resp, err := model.GenerateContent(ctx, genai.Text(llmSystemPrompt+"\n\nInstruction: "+instruction))
	if err != nil {
		return nil, err
	}

	raw, err := extractResponseText(resp)
	if err != nil {
		return nil, err
	}
	raw = stripJSONFences(raw)

	var subs []Subtask
	if err := json.Unmarshal([]byte(raw), &subs); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	if len(subs) == 0 {
		return nil, errors.New("no subtasks in array")
	}
	return normalizeSubtasks(subs), nil
}

func extractResponseText(resp *genai.GenerateContentResponse) (string, error) {
	if resp == nil || len(resp.Candidates) == 0 {
		return "", errors.New("no candidates")
	}
	cand := resp.Candidates[0]
	if cand == nil || cand.Content == nil || len(cand.Content.Parts) == 0 {
		return "", errors.New("empty candidate content")
	}
	var b strings.Builder
	for _, p := range cand.Content.Parts {
		if t, ok := p.(genai.Text); ok {
			b.WriteString(string(t))
		}
	}
	if b.Len() == 0 {
		return "", errors.New("no text parts")
	}
	return b.String(), nil
}

func stripJSONFences(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(s), "json") {
		s = strings.TrimSpace(s[4:])
	}
	if i := strings.LastIndex(s, "```"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func normalizeSubtasks(subs []Subtask) []Subtask {
	sort.SliceStable(subs, func(i, j int) bool {
		oi, oj := subs[i].Order, subs[j].Order
		if oi == 0 && oj == 0 {
			return i < j
		}
		if oi == 0 {
			return false
		}
		if oj == 0 {
			return true
		}
		return oi < oj
	})
	out := make([]Subtask, 0, len(subs))
	for _, st := range subs {
		name := strings.TrimSpace(st.Name)
		desc := strings.TrimSpace(st.Description)
		if name == "" && desc == "" {
			continue
		}
		cp := st
		out = append(out, cp)
	}
	for i := range out {
		out[i].Order = i + 1
	}
	return out
}

func buildInstructionPayload(st Subtask) string {
	name := strings.TrimSpace(st.Name)
	desc := strings.TrimSpace(st.Description)
	meta := ""
	if st.EstimatedDurationSeconds > 0 {
		meta = fmt.Sprintf(" [estimated_duration_seconds=%d]", st.EstimatedDurationSeconds)
	}
	switch {
	case name != "" && desc != "":
		return name + ": " + desc + meta
	case desc != "":
		return desc + meta
	default:
		return name + meta
	}
}

func subtasksToMessages(jobID, priority string, subs []Subtask) []models.SubtaskMessage {
	out := make([]models.SubtaskMessage, 0, len(subs))
	for i, st := range subs {
		id := strings.TrimSpace(st.ID)
		if id == "" {
			id = fmt.Sprintf("%s-st-%02d", jobID, i+1)
		}
		out = append(out, models.SubtaskMessage{
			JobID:       jobID,
			SubtaskID:   id,
			Instruction: buildInstructionPayload(st),
			Order:       st.Order,
			Priority:    priority,
			Attempt:     1,
		})
	}
	return out
}

func tryLLMDecompose(jobID, instruction, priority, apiKey string) ([]models.SubtaskMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	subs, err := planWithGemini(ctx, apiKey, instruction)
	if err != nil {
		return nil, err
	}
	msgs := subtasksToMessages(jobID, priority, subs)
	if len(msgs) == 0 {
		return nil, errors.New("no messages after conversion")
	}
	return msgs, nil
}
