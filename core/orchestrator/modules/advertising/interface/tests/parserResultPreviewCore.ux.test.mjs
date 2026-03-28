import test from 'node:test';
import assert from 'node:assert/strict';

import { buildParserDraftPreviewUxState } from '../desk/tabs/parserResultPreviewCore.js';

test('parser draft preview ux state: run-level success stays ready even if output detail failed', () => {
  const ready = buildParserDraftPreviewUxState({
    previewState: { mode: 'preview_failed', statusDescription: 'output mapping failed', isStalePreview: false },
    inputSource: {
      label: 'Источник входа для preview: server preview-run',
      description: 'Один и тот же server preview-run для input/output.'
    },
    hasFreshPreviewRun: true
  });

  assert.equal(ready.state, 'preview_ready');
  assert.equal(ready.actionLabel, 'Обновить предпросмотр');
});
