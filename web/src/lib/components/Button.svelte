<script lang="ts">
  export let kind: 'primary' | 'secondary' | 'ghost' | 'danger' = 'secondary';
  export let type: 'button' | 'submit' = 'button';
  export let disabled = false;
</script>

<button class={`button ${kind}`} {type} {disabled} on:click on:submit on:keydown on:keyup on:mouseenter on:mouseleave on:focus on:blur {...$$restProps}>
  <slot />
</button>

<style>
  .button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    min-height: 42px;
    padding: 0 14px;
    border-radius: 14px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    cursor: pointer;
    white-space: nowrap;
    box-shadow: 0 8px 24px hsl(215 50% 2% / 0.18);
    transition: background 0.15s ease, border-color 0.15s ease, opacity 0.15s ease, transform 0.15s ease, box-shadow 0.15s ease;
  }

  .button:hover:not(:disabled) {
    transform: translateY(-1px);
    border-color: hsl(0 0% 100% / 0.18);
  }

  .button:active:not(:disabled) {
    transform: translateY(0);
  }

  .button:focus-visible {
    outline: none;
    box-shadow:
      0 0 0 3px hsl(var(--primary) / 0.18),
      0 10px 28px hsl(215 50% 2% / 0.24);
  }

  .button:disabled {
    opacity: 0.55;
    cursor: default;
    box-shadow: none;
  }

  .primary {
    background: hsl(var(--primary));
    border-color: hsl(var(--primary));
    color: hsl(var(--primary-foreground));
    font-weight: 700;
  }

  .secondary {
    background: hsl(0 0% 100% / 0.05);
    color: hsl(var(--foreground));
  }

  .secondary:hover:not(:disabled) {
    background: hsl(0 0% 100% / 0.09);
  }

  .ghost {
    background: transparent;
    color: hsl(var(--muted-foreground));
  }

  .ghost:hover:not(:disabled) {
    background: hsl(0 0% 100% / 0.04);
    color: hsl(var(--foreground));
  }

  .danger {
    background: hsl(var(--danger) / 0.16);
    border-color: hsl(var(--danger) / 0.32);
    color: hsl(0 96% 82%);
  }

  .danger:hover:not(:disabled) {
    background: hsl(var(--danger) / 0.24);
    border-color: hsl(var(--danger) / 0.42);
  }
</style>
