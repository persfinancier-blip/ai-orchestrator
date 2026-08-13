<script>
  import { createEventDispatcher } from 'svelte';
  const dispatch = createEventDispatcher();
  let token = '';
  let loading = false;
  let error = '';

  async function login() {
    loading = true;
    error = '';
    try {
      const res = await fetch('/ai-orchestrator/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token })
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(payload?.error === 'auth_not_configured' ? 'На сервере не настроен AO_CONTROL_TOKEN.' : 'Неверный ключ доступа.');
      token = '';
      dispatch('authenticated');
    } catch (e) {
      error = String(e?.message || e || 'Ошибка входа');
    } finally {
      loading = false;
    }
  }
</script>

<div class="login">
  <form on:submit|preventDefault={login}>
    <div class="logo">AO</div>
    <small>AI ORCHESTRATOR</small>
    <h1>Вход в рабочее пространство</h1>
    <p>Введите ключ администратора. После входа ключ не хранится в браузерном JavaScript — сервер выдаёт подписанную HttpOnly-сессию.</p>
    <label>Ключ доступа<input type="password" bind:value={token} autocomplete="current-password" placeholder="AO_CONTROL_TOKEN" autofocus /></label>
    {#if error}<div class="error">{error}</div>{/if}
    <button disabled={loading || !token.trim()}>{loading ? 'Проверяем…' : 'Войти'}</button>
  </form>
</div>

<style>
  .login { min-height: 100vh; display: grid; place-items: center; padding: 24px; box-sizing: border-box; background: #f4f6f9; color: #172033; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
  form { width: min(420px, 100%); padding: 34px; box-sizing: border-box; border: 1px solid #e3e8f0; border-radius: 20px; background: #fff; box-shadow: 0 20px 50px rgba(15,23,42,.08); }
  .logo { width: 42px; height: 42px; border-radius: 12px; display: grid; place-items: center; background: #172033; color: #fff; font-size: 12px; font-weight: 900; margin-bottom: 20px; }
  form > small { color: #94a3b8; font-size: 9px; font-weight: 900; letter-spacing: .14em; }
  h1 { margin: 7px 0 10px; font-size: 26px; letter-spacing: -.03em; } p { margin: 0 0 24px; color: #64748b; font-size: 12px; line-height: 1.55; }
  label { display: flex; flex-direction: column; gap: 7px; color: #475569; font-size: 11px; font-weight: 750; }
  input { border: 1px solid #dce3ec; border-radius: 10px; padding: 12px; outline: none; font: inherit; } input:focus { border-color: #64748b; }
  button { width: 100%; margin-top: 14px; border: 0; border-radius: 10px; padding: 12px; background: #172033; color: #fff; font-weight: 800; cursor: pointer; } button:disabled { opacity: .45; cursor: default; }
  .error { margin-top: 12px; padding: 10px; border-radius: 8px; background: #fff1f2; color: #be123c; font-size: 11px; }
</style>
