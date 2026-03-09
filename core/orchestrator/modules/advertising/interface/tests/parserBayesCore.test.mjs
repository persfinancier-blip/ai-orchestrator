import test from 'node:test';
import assert from 'node:assert/strict';

import { analyzeInputCandidatesByBayes, analyzeJoinPairsByBayes } from '../server/parserBayesCore.mjs';

test('parser bayes: recommends business array node instead of wrapper root', () => {
  const payload = {
    success: true,
    failed: 0,
    total_requests: 1,
    list: [
      { id: '1', title: 'Alpha', price: 10, status: 'active' },
      { id: '2', title: 'Beta', price: 15, status: 'paused' }
    ],
    total: '2'
  };

  const result = analyzeInputCandidatesByBayes(payload);
  assert.equal(result.recommended_candidate.path, 'list');
  assert.ok(result.recommended_candidate.probability > 0.5);
  assert.equal(result.recommended_candidate.top_hypothesis.code, 'working_set');
});

test('parser bayes: recognizes JSON string and HTML as separate hypotheses', () => {
  const jsonCandidate = analyzeInputCandidatesByBayes('{"list":[{"id":"1","title":"A"}]}');
  const htmlCandidate = analyzeInputCandidatesByBayes('<html><body>Access denied</body></html>');

  assert.equal(jsonCandidate.candidates[0].top_hypothesis.code, 'json_string');
  assert.equal(htmlCandidate.candidates[0].top_hypothesis.code, 'html_error');
});

test('parser bayes: recommends join fields by probability, not by hardcoded equality only', () => {
  const sourceRows = [
    { campaign_id: '101', shop: 'A', price: 10 },
    { campaign_id: '102', shop: 'B', price: 15 },
    { campaign_id: '103', shop: 'C', price: 20 }
  ];
  const lookupRows = [
    { campaign_id: '101', title: 'Campaign 101', state: 'active' },
    { campaign_id: '102', title: 'Campaign 102', state: 'paused' },
    { campaign_id: '103', title: 'Campaign 103', state: 'active' }
  ];

  const result = analyzeJoinPairsByBayes(sourceRows, lookupRows);
  assert.equal(result.recommended_rules[0].sourceField, 'campaign_id');
  assert.equal(result.recommended_rules[0].targetField, 'campaign_id');
  assert.ok(result.recommended_rules[0].probability > 0.5);
  assert.equal(result.suggestions[0].sourceField, 'campaign_id');
});
