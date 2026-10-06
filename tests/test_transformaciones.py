from utils_transform import limpiar_monto_transaccion

def test_limpiar_monto_transaccion_valido():
    assert limpiar_monto_transaccion(150.5) == 150.5

def test_limpiar_monto_transaccion_negativo():
    assert limpiar_monto_transaccion(-50) == 0

def test_limpiar_monto_transaccion_invalido():
    assert limpiar_monto_transaccion('invalid_amount') == 0
    assert limpiar_monto_transaccion(None) == 0