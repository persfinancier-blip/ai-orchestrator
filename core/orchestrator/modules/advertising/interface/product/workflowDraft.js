function clean(value) {
  return String(value || '').trim();
}

function safeCode(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 48) || 'assistant_process';
}

export function workflowDraftForApi({ apiStoreId, name = 'Assistant workflow', description = '' } = {}) {
  const storeId = Math.trunc(Number(apiStoreId || 0));
  if (!(storeId > 0)) return { ok: false, reason: 'missing_api_store_id', payload: null };

  const processCode = safeCode(name);
  const nodes = [
    {
      id: 'start_1',
      type: 'tool',
      x: 100,
      y: 180,
      config: {
        name: 'Запуск',
        toolType: 'start_process',
        settings: {
          isEnabled: false,
          triggerType: 'manual',
          runPolicy: 'single_instance',
          processCode,
          executionScopeMode: 'single_global'
        }
      }
    },
    {
      id: 'api_1',
      type: 'tool',
      x: 390,
      y: 180,
      config: {
        name: 'API запрос',
        toolType: 'api_request',
        settings: {
          templateStoreId: storeId,
          templateId: String(storeId)
        }
      }
    },
    {
      id: 'end_1',
      type: 'tool',
      x: 680,
      y: 180,
      config: { name: 'Готово', toolType: 'end_process', settings: {} }
    }
  ];

  return {
    ok: true,
    reason: '',
    payload: {
      desk_name: clean(name) || 'Assistant workflow',
      desk_type: 'data',
      description: clean(description) || 'Draft generated from Integration Assistant. Review and publish manually.',
      schema_version: 1,
      is_active: false,
      updated_by: 'product_assistant',
      config_json: {
        nodes,
        edges: [
          { id: 'edge_start_api', from: 'start_1', to: 'api_1', fromPort: 'out', toPort: 'in' },
          { id: 'edge_api_end', from: 'api_1', to: 'end_1', fromPort: 'out', toPort: 'in' }
        ],
        viewport: { panX: 0, panY: 0, zoom: 1 }
      }
    }
  };
}
