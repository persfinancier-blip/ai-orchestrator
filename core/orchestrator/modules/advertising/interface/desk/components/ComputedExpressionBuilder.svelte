<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import {
    COMPUTED_FUNCTION_CATEGORIES,
    COMPUTED_FUNCTIONS_BY_KEY,
    CONDITION_OPERATOR_OPTIONS,
    VALUE_SOURCE_OPTIONS,
    buildComputedFieldTypeWarnings,
    buildExpressionFromBuilder,
    createExpressionBuilder,
    createExpressionOperand,
    functionsByCategory,
    normalizeExpressionBuilder,
    normalizeExpressionOperand
  } from '../tabs/computedFieldBuilderCore.js';

  export let builder: any = {};
  export let availableFields: Array<{ name: string; type?: string }> = [];
  export let fieldTypes: Record<string, string> = {};
  export let compact = false;
  export let depth = 0;
  export let maxDepth = 2;

  const dispatch = createEventDispatcher<{ change: { builder: any } }>();

  $: currentBuilder = normalizeExpressionBuilder(builder);
  $: definition = COMPUTED_FUNCTIONS_BY_KEY[currentBuilder.functionKey] || COMPUTED_FUNCTIONS_BY_KEY.add;
  $: functionOptions = functionsByCategory(currentBuilder.category);
  $: expressionPreview = buildExpressionFromBuilder(currentBuilder);
  $: typeWarnings = buildComputedFieldTypeWarnings(currentBuilder, fieldTypes);

  function emit(nextBuilder: any) {
    dispatch('change', { builder: normalizeExpressionBuilder(nextBuilder) });
  }

  function patchBuilder(patch: Record<string, any>) {
    emit({ ...currentBuilder, ...patch });
  }

  function changeCategory(category: string) {
    const nextFunction = functionsByCategory(category)[0]?.key || 'add';
    emit(createExpressionBuilder(nextFunction));
  }

  function changeFunction(functionKey: string) {
    emit(createExpressionBuilder(functionKey));
  }

  function updateArg(index: number, patch: Record<string, any>) {
    const args = currentBuilder.args.map((arg: any, idx: number) =>
      idx === index ? normalizeExpressionOperand({ ...arg, ...patch }) : normalizeExpressionOperand(arg)
    );
    patchBuilder({ args });
  }

  function replaceArg(index: number, nextOperand: any) {
    const args = currentBuilder.args.map((arg: any, idx: number) =>
      idx === index ? normalizeExpressionOperand(nextOperand) : normalizeExpressionOperand(arg)
    );
    patchBuilder({ args });
  }

  function addArg() {
    patchBuilder({ args: [...currentBuilder.args, createExpressionOperand(definition.allowedKinds?.includes('number') ? 'number' : 'field')] });
  }

  function removeArg(index: number) {
    const minArgs = Math.max(2, Number(definition.minArgs || 2));
    if (currentBuilder.args.length <= minArgs) return;
    patchBuilder({ args: currentBuilder.args.filter((_arg: any, idx: number) => idx !== index) });
  }

  function updateCondition(part: 'left' | 'right', patch: Record<string, any>) {
    patchBuilder({
      condition: {
        ...currentBuilder.condition,
        [part]: normalizeExpressionOperand({ ...(currentBuilder.condition?.[part] || createExpressionOperand('field')), ...patch })
      }
    });
  }

  function replaceConditionOperand(part: 'left' | 'right', operand: any) {
    patchBuilder({
      condition: {
        ...currentBuilder.condition,
        [part]: normalizeExpressionOperand(operand)
      }
    });
  }

  function updateConditionOperator(operator: string) {
    patchBuilder({
      condition: {
        ...currentBuilder.condition,
        operator
      }
    });
  }

  function updateBranch(branch: 'whenTrue' | 'whenFalse', patch: Record<string, any>) {
    patchBuilder({
      [branch]: normalizeExpressionOperand({ ...(currentBuilder[branch] || createExpressionOperand('field')), ...patch })
    });
  }

  function replaceBranchOperand(branch: 'whenTrue' | 'whenFalse', operand: any) {
    patchBuilder({
      [branch]: normalizeExpressionOperand(operand)
    });
  }

  function operandNeedsNestedBuilder(operand: any) {
    return normalizeExpressionOperand(operand).sourceType === 'expression' && depth < maxDepth;
  }

  function conditionNeedsRight(operator: string) {
    return !['empty', 'not_empty'].includes(String(operator || '').trim());
  }

  function fieldLabel(field: { name: string; type?: string }) {
    return field?.type ? `${field.name} (${field.type})` : field.name;
  }

  function selectValue(event: Event) {
    return (event.currentTarget as HTMLSelectElement).value;
  }

  function inputValue(event: Event) {
    return (event.currentTarget as HTMLInputElement).value;
  }
</script>

<div class:computed-builder={true} class:computed-builder-compact={compact} class:computed-builder-nested={depth > 0}>
  <div class="computed-builder-head">
    <label>
      Категория
      <select value={currentBuilder.category} on:change={(e) => changeCategory(selectValue(e))}>
        {#each COMPUTED_FUNCTION_CATEGORIES as item}
          <option value={item.id}>{item.label}</option>
        {/each}
      </select>
    </label>
    <label>
      Функция
      <select value={currentBuilder.functionKey} on:change={(e) => changeFunction(selectValue(e))}>
        {#each functionOptions as item}
          <option value={item.key}>{item.label}</option>
        {/each}
      </select>
    </label>
  </div>

  <div class="hint">{definition.description}</div>

  {#if definition.argMode === 'if'}
    <div class="builder-block">
      <div class="builder-block-title">Условие</div>
      <div class="builder-condition-row">
        <div class="builder-value">
          <label>Левый операнд</label>
          <select
            value={currentBuilder.condition.left.sourceType}
            on:change={(e) => updateCondition('left', { sourceType: selectValue(e) })}
          >
            {#each VALUE_SOURCE_OPTIONS as item}
              <option value={item.value} disabled={item.value === 'expression' && depth >= maxDepth}>{item.label}</option>
            {/each}
          </select>
          {#if currentBuilder.condition.left.sourceType === 'field'}
            <select value={currentBuilder.condition.left.field} on:change={(e) => updateCondition('left', { field: selectValue(e) })}>
              <option value="">Выбери поле</option>
              {#each availableFields as field}
                <option value={field.name}>{fieldLabel(field)}</option>
              {/each}
            </select>
          {:else if currentBuilder.condition.left.sourceType === 'number'}
            <input type="number" value={currentBuilder.condition.left.numberValue || ''} on:input={(e) => updateCondition('left', { numberValue: inputValue(e) })} />
          {:else if currentBuilder.condition.left.sourceType === 'text'}
            <input value={currentBuilder.condition.left.textValue || ''} on:input={(e) => updateCondition('left', { textValue: inputValue(e) })} />
          {:else if currentBuilder.condition.left.sourceType === 'boolean'}
            <select value={currentBuilder.condition.left.booleanValue || 'false'} on:change={(e) => updateCondition('left', { booleanValue: selectValue(e) })}>
              <option value="true">Истина</option>
              <option value="false">Ложь</option>
            </select>
          {:else if currentBuilder.condition.left.sourceType === 'date'}
            <input type="date" value={currentBuilder.condition.left.dateValue || ''} on:input={(e) => updateCondition('left', { dateValue: inputValue(e) })} />
          {:else if operandNeedsNestedBuilder(currentBuilder.condition.left)}
            <svelte:self
              compact={true}
              depth={depth + 1}
              maxDepth={maxDepth}
              builder={currentBuilder.condition.left.builder}
              availableFields={availableFields}
              fieldTypes={fieldTypes}
              on:change={(e) => replaceConditionOperand('left', { ...currentBuilder.condition.left, builder: e.detail.builder })}
            />
          {/if}
        </div>
        <div class="builder-operator">
          <label>Оператор</label>
          <select value={currentBuilder.condition.operator} on:change={(e) => updateConditionOperator(selectValue(e))}>
            {#each CONDITION_OPERATOR_OPTIONS as item}
              <option value={item.value}>{item.label}</option>
            {/each}
          </select>
        </div>
        {#if conditionNeedsRight(currentBuilder.condition.operator)}
          <div class="builder-value">
            <label>Правый операнд</label>
            <select
              value={currentBuilder.condition.right.sourceType}
              on:change={(e) => updateCondition('right', { sourceType: selectValue(e) })}
            >
              {#each VALUE_SOURCE_OPTIONS as item}
                <option value={item.value} disabled={item.value === 'expression' && depth >= maxDepth}>{item.label}</option>
              {/each}
            </select>
            {#if currentBuilder.condition.right.sourceType === 'field'}
              <select value={currentBuilder.condition.right.field} on:change={(e) => updateCondition('right', { field: selectValue(e) })}>
                <option value="">Выбери поле</option>
                {#each availableFields as field}
                  <option value={field.name}>{fieldLabel(field)}</option>
                {/each}
              </select>
            {:else if currentBuilder.condition.right.sourceType === 'number'}
              <input type="number" value={currentBuilder.condition.right.numberValue || ''} on:input={(e) => updateCondition('right', { numberValue: inputValue(e) })} />
            {:else if currentBuilder.condition.right.sourceType === 'text'}
              <input value={currentBuilder.condition.right.textValue || ''} on:input={(e) => updateCondition('right', { textValue: inputValue(e) })} />
            {:else if currentBuilder.condition.right.sourceType === 'boolean'}
              <select value={currentBuilder.condition.right.booleanValue || 'false'} on:change={(e) => updateCondition('right', { booleanValue: selectValue(e) })}>
                <option value="true">Истина</option>
                <option value="false">Ложь</option>
              </select>
            {:else if currentBuilder.condition.right.sourceType === 'date'}
              <input type="date" value={currentBuilder.condition.right.dateValue || ''} on:input={(e) => updateCondition('right', { dateValue: inputValue(e) })} />
            {:else if operandNeedsNestedBuilder(currentBuilder.condition.right)}
              <svelte:self
                compact={true}
                depth={depth + 1}
                maxDepth={maxDepth}
                builder={currentBuilder.condition.right.builder}
                availableFields={availableFields}
                fieldTypes={fieldTypes}
                on:change={(e) => replaceConditionOperand('right', { ...currentBuilder.condition.right, builder: e.detail.builder })}
              />
            {/if}
          </div>
        {/if}
      </div>

      <div class="builder-condition-row builder-condition-result-row">
        <div class="builder-value">
          <label>Если условие истинно</label>
          <select
            value={currentBuilder.whenTrue.sourceType}
            on:change={(e) => updateBranch('whenTrue', { sourceType: selectValue(e) })}
          >
            {#each VALUE_SOURCE_OPTIONS as item}
              <option value={item.value} disabled={item.value === 'expression' && depth >= maxDepth}>{item.label}</option>
            {/each}
          </select>
          {#if currentBuilder.whenTrue.sourceType === 'field'}
            <select value={currentBuilder.whenTrue.field} on:change={(e) => updateBranch('whenTrue', { field: selectValue(e) })}>
              <option value="">Выбери поле</option>
              {#each availableFields as field}
                <option value={field.name}>{fieldLabel(field)}</option>
              {/each}
            </select>
          {:else if currentBuilder.whenTrue.sourceType === 'number'}
            <input type="number" value={currentBuilder.whenTrue.numberValue || ''} on:input={(e) => updateBranch('whenTrue', { numberValue: inputValue(e) })} />
          {:else if currentBuilder.whenTrue.sourceType === 'text'}
            <input value={currentBuilder.whenTrue.textValue || ''} on:input={(e) => updateBranch('whenTrue', { textValue: inputValue(e) })} />
          {:else if currentBuilder.whenTrue.sourceType === 'boolean'}
            <select value={currentBuilder.whenTrue.booleanValue || 'false'} on:change={(e) => updateBranch('whenTrue', { booleanValue: selectValue(e) })}>
              <option value="true">Истина</option>
              <option value="false">Ложь</option>
            </select>
          {:else if currentBuilder.whenTrue.sourceType === 'date'}
            <input type="date" value={currentBuilder.whenTrue.dateValue || ''} on:input={(e) => updateBranch('whenTrue', { dateValue: inputValue(e) })} />
          {:else if operandNeedsNestedBuilder(currentBuilder.whenTrue)}
            <svelte:self
              compact={true}
              depth={depth + 1}
              maxDepth={maxDepth}
              builder={currentBuilder.whenTrue.builder}
              availableFields={availableFields}
              fieldTypes={fieldTypes}
              on:change={(e) => replaceBranchOperand('whenTrue', { ...currentBuilder.whenTrue, builder: e.detail.builder })}
            />
          {/if}
        </div>
        <div class="builder-value">
          <label>Если условие ложно</label>
          <select
            value={currentBuilder.whenFalse.sourceType}
            on:change={(e) => updateBranch('whenFalse', { sourceType: selectValue(e) })}
          >
            {#each VALUE_SOURCE_OPTIONS as item}
              <option value={item.value} disabled={item.value === 'expression' && depth >= maxDepth}>{item.label}</option>
            {/each}
          </select>
          {#if currentBuilder.whenFalse.sourceType === 'field'}
            <select value={currentBuilder.whenFalse.field} on:change={(e) => updateBranch('whenFalse', { field: selectValue(e) })}>
              <option value="">Выбери поле</option>
              {#each availableFields as field}
                <option value={field.name}>{fieldLabel(field)}</option>
              {/each}
            </select>
          {:else if currentBuilder.whenFalse.sourceType === 'number'}
            <input type="number" value={currentBuilder.whenFalse.numberValue || ''} on:input={(e) => updateBranch('whenFalse', { numberValue: inputValue(e) })} />
          {:else if currentBuilder.whenFalse.sourceType === 'text'}
            <input value={currentBuilder.whenFalse.textValue || ''} on:input={(e) => updateBranch('whenFalse', { textValue: inputValue(e) })} />
          {:else if currentBuilder.whenFalse.sourceType === 'boolean'}
            <select value={currentBuilder.whenFalse.booleanValue || 'false'} on:change={(e) => updateBranch('whenFalse', { booleanValue: selectValue(e) })}>
              <option value="true">Истина</option>
              <option value="false">Ложь</option>
            </select>
          {:else if currentBuilder.whenFalse.sourceType === 'date'}
            <input type="date" value={currentBuilder.whenFalse.dateValue || ''} on:input={(e) => updateBranch('whenFalse', { dateValue: inputValue(e) })} />
          {:else if operandNeedsNestedBuilder(currentBuilder.whenFalse)}
            <svelte:self
              compact={true}
              depth={depth + 1}
              maxDepth={maxDepth}
              builder={currentBuilder.whenFalse.builder}
              availableFields={availableFields}
              fieldTypes={fieldTypes}
              on:change={(e) => replaceBranchOperand('whenFalse', { ...currentBuilder.whenFalse, builder: e.detail.builder })}
            />
          {/if}
        </div>
      </div>
    </div>
  {:else if definition.argMode === 'none'}
    <div class="inline-hint">У этой функции нет аргументов. Значение вычисляется автоматически.</div>
  {:else}
    <div class="builder-block">
      {#each currentBuilder.args as arg, index}
        <div class="builder-operand-row">
          <div class="builder-value">
            <label>{definition.argMode === 'fixed' ? definition.args[index]?.label || `Аргумент ${index + 1}` : `Операнд ${index + 1}`}</label>
            <select value={arg.sourceType} on:change={(e) => updateArg(index, { sourceType: selectValue(e) })}>
              {#each VALUE_SOURCE_OPTIONS as item}
                <option value={item.value} disabled={item.value === 'expression' && depth >= maxDepth}>{item.label}</option>
              {/each}
            </select>

            {#if arg.sourceType === 'field'}
              <select value={arg.field} on:change={(e) => updateArg(index, { field: selectValue(e) })}>
                <option value="">Выбери поле</option>
                {#each availableFields as field}
                  <option value={field.name}>{fieldLabel(field)}</option>
                {/each}
              </select>
            {:else if arg.sourceType === 'number'}
              <input type="number" value={arg.numberValue || ''} on:input={(e) => updateArg(index, { numberValue: inputValue(e) })} />
            {:else if arg.sourceType === 'text'}
              <input value={arg.textValue || ''} on:input={(e) => updateArg(index, { textValue: inputValue(e) })} />
            {:else if arg.sourceType === 'boolean'}
              <select value={arg.booleanValue || 'false'} on:change={(e) => updateArg(index, { booleanValue: selectValue(e) })}>
                <option value="true">Истина</option>
                <option value="false">Ложь</option>
              </select>
            {:else if arg.sourceType === 'date'}
              <input type="date" value={arg.dateValue || ''} on:input={(e) => updateArg(index, { dateValue: inputValue(e) })} />
            {:else if operandNeedsNestedBuilder(arg)}
              <svelte:self
                compact={true}
                depth={depth + 1}
                maxDepth={maxDepth}
                builder={arg.builder}
                availableFields={availableFields}
                fieldTypes={fieldTypes}
                on:change={(e) => replaceArg(index, { ...arg, builder: e.detail.builder })}
              />
            {/if}
          </div>

          {#if definition.argMode === 'variadic'}
            <button type="button" class="icon-btn danger-icon-btn operand-remove-btn" on:click={() => removeArg(index)} disabled={currentBuilder.args.length <= Math.max(2, definition.minArgs || 2)}>x</button>
          {/if}
        </div>
      {/each}

      {#if definition.argMode === 'variadic'}
        <button type="button" class="mini-btn" on:click={addArg}>Операнд +</button>
      {/if}
    </div>
  {/if}

  {#if typeWarnings.length}
    <div class="computed-builder-warnings">
      {#each typeWarnings as warning}
        <div class="inline-hint">{warning}</div>
      {/each}
    </div>
  {/if}

  <div class="computed-preview-box">
    <div class="computed-preview-label">Предпросмотр выражения</div>
    <code>{expressionPreview}</code>
  </div>
</div>

<style>
  .computed-builder {
    display: flex;
    flex-direction: column;
    gap: 10px;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 12px;
    background: #f8fafc;
  }
  .computed-builder-compact {
    padding: 10px;
    background: #fff;
  }
  .computed-builder-nested {
    border-style: dashed;
  }
  .computed-builder-head {
    display: grid;
    grid-template-columns: minmax(180px, 0.8fr) minmax(220px, 1fr);
    gap: 10px;
  }
  .builder-block,
  .builder-value {
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
  }
  .builder-condition-row,
  .builder-operand-row {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
    align-items: start;
  }
  .builder-condition-row {
    grid-template-columns: minmax(0, 1fr) 220px minmax(0, 1fr);
  }
  .builder-condition-result-row {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .builder-operator {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .builder-block-title,
  .computed-preview-label {
    font-size: 11px;
    font-weight: 600;
    color: #64748b;
  }
  .computed-preview-box {
    display: flex;
    flex-direction: column;
    gap: 6px;
    background: #fff;
    border: 1px solid #dbe4f0;
    border-radius: 10px;
    padding: 10px;
  }
  .computed-preview-box code {
    white-space: pre-wrap;
    word-break: break-word;
    font-size: 12px;
    color: #0f172a;
  }
  .operand-remove-btn {
    align-self: end;
  }
  @media (max-width: 1120px) {
    .computed-builder-head,
    .builder-condition-row,
    .builder-operand-row,
    .builder-condition-result-row {
      grid-template-columns: 1fr;
    }
  }
</style>
