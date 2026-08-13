export function buildSimpleApiWorkflow({ name = 'Сценарий', apiStoreId, containerId = null } = {}) {
  const storeId = Math.trunc(Number(apiStoreId || 0));
  if (!(storeId > 0)) throw new Error('api_store_id_required');
  const startId = 'start_1';
  const apiId = 'api_1';
  const endId = 'end_1';
  const startSettings = {
    isEnabled: false,
    triggerType: 'manual',
    processCode: '',
    runPolicy: 'single_instance',
    executionScopeMode: 'single_global',
    scopeType: 'global',
    scopeRef: 'global',
    contextJson: containerId ? JSON.stringify({ container_id: Math.trunc(Number(containerId)) }) : '{}'
  };
  return {
    nodes: [
      { id:startId, type:'tool', x:100, y:180, config:{ name:'Старт', toolType:'start_process', settings:startSettings } },
      { id:apiId, type:'tool', x:390, y:180, config:{ name:String(name || 'API'), toolType:'api_request', settings:{ templateId:storeId, templateStoreId:storeId } } },
      { id:endId, type:'tool', x:680, y:180, config:{ name:'Финиш', toolType:'end_process', settings:{} } }
    ],
    edges: [
      { id:'e_start_api', from:startId, to:apiId, fromPort:'out', toPort:'in' },
      { id:'e_api_end', from:apiId, to:endId, fromPort:'out', toPort:'in' }
    ],
    viewport:{ x:0, y:0, scale:1 },
    selectedNodeId:null,
    settings:{ workflowLog:{ enabled:true, target:{ templateId:'builtin_bronze_system_workflow_log', schema:'', table:'' } } }
  };
}
