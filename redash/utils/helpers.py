
def get_cache(max_age: int, max_cache_time: int) -> int:
    """
    Determina el tiempo máximo de vida de un caché dado dos parámetros.

    Args:
        max_age (int): Edad máxima que puede tener el caché definido por el usuario.
        max_cache_time (int): Tiempo máximo que un elemento puede permanecer en el caché, definido por configuración.

    Returns:
        int: El mayor de los dos valores proporcionados, determinando así el tiempo de expiración del caché.

    Ejemplo:
        Si max_age es 0 y max_cache_time es 300, la función retornará 300.
    """
    return max(max_age, max_cache_time)
