<script>
  export let articleId = null;
  export let selectedArticle = null;
  
  // Пример данных для таблицы ставок по кластерам
  let clusterBids = [
    { cluster: 'Кроссовки', bid: 150, maxBid: 200, status: 'active' },
    { cluster: 'Футболки', bid: 120, maxBid: 180, status: 'active' },
    { cluster: 'Шорты', bid: 90, maxBid: 150, status: 'paused' },
    { cluster: 'Кепки', bid: 80, maxBid: 120, status: 'active' }
  ];
  
  function handleBidChange(cluster, newBid) {
    // Логика изменения ставки
    console.log(`Changing bid for ${cluster} to ${newBid}`);
  }
  
  function toggleCluster(cluster) {
    // Логика включения/выключения кластера
    console.log(`Toggling cluster ${cluster}`);
  }
</script>

<div class="cluster-bids-table">
  <div class="table-header">
    <h4>Ставки по кластерам</h4>
  </div>
  
  <div class="table-container">
    <table class="bids-table">
      <thead>
        <tr>
          <th>Кластер</th>
          <th>Ставка</th>
          <th>Макс. ставка</th>
          <th>Статус</th>
          <th>Действия</th>
        </tr>
      </thead>
      <tbody>
        {#each clusterBids as item}
          <tr>
            <td>{item.cluster}</td>
            <td>
              <input 
                type="number" 
                value={item.bid} 
                class="bid-input"
                on:change={(e) => handleBidChange(item.cluster, e.target.value)}
              />
            </td>
            <td>{item.maxBid}</td>
            <td>
              <span class="status-badge" class:active={item.status === 'active'}>
                {item.status === 'active' ? 'Активен' : 'Пауза'}
              </span>
            </td>
            <td>
              <button 
                class="toggle-button" 
                on:click={() => toggleCluster(item.cluster)}
              >
                {item.status === 'active' ? 'Пауза' : 'Включить'}
              </button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>

<style>
  .cluster-bids-table {
    padding: 16px 0;
  }
  
  .table-header h4 {
    margin: 0 0 16px 0;
    font-size: 16px;
    font-weight: 600;
    color: #000000;
  }
  
  .table-container {
    overflow-x: auto;
  }
  
  .bids-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }
  
  .bids-table th {
    padding: 12px 16px;
    text-align: left;
    font-weight: 600;
    color: #86868b;
    border-bottom: 1px solid #d2d2d7;
  }
  
  .bids-table td {
    padding: 12px 16px;
    border-bottom: 1px solid #d2d2d7;
    vertical-align: middle;
  }
  
  .bid-input {
    width: 80px;
    padding: 6px 8px;
    border: 1px solid #d2d2d7;
    border-radius: 4px;
    font-size: 14px;
  }
  
  .status-badge {
    padding: 4px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 500;
    background-color: #f5f5f7;
    color: #86868b;
  }
  
  .status-badge.active {
    background-color: #e3f2fd;
    color: #0071e3;
  }
  
  .toggle-button {
    padding: 6px 12px;
    background-color: white;
    color: #0071e3;
    border: 1px solid #d2d2d7;
    border-radius: 6px;
    font-size: 12px;
    cursor: pointer;
    transition: all 0.2s ease-out;
  }
  
  .toggle-button:hover {
    background-color: #f5f5f7;
    border-color: #c0c0c4;
  }
</style>