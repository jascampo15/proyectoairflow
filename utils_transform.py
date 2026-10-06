def limpiar_monto_transaccion(valor):
    if valor is None:
        return 0
    try:
        val = float(valor)
        return max(0, val)
    except (ValueError, TypeError):
        return 0