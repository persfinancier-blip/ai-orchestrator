<script>
  import { createEventDispatcher } from 'svelte';
  
  export let options = [];
  export let value = '';
  
  const dispatch = createEventDispatcher();
  
  function handleChange(optionValue) {
    dispatch('change', { value: optionValue });
  }
</script>

<div class="radio-toggle-group">
  {#each options as option}
    <label class="radio-toggle">
      <input 
        type="radio"
        name="radio-group"
        value={option.id}
        checked={value === option.id}
        on:change={() => handleChange(option.id)}
      />
      <span class="radio-toggle-text">{option.label}</span>
    </label>
  {/each}
</div>

<style>
  .radio-toggle-group {
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-left: 24px;
  }
  
  .radio-toggle {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: #000000;
    cursor: pointer;
  }
  
  .radio-toggle input[type="radio"] {
    width: 16px;
    height: 16px;
    accent-color: #0071e3;
  }
  
  .radio-toggle-text {
    user-select: none;
  }
</style>