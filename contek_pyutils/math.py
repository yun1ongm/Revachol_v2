import numpy as np
from expression import Nothing, Option, Some


def np_inf_to_nan(arr: np.ndarray) -> Option[np.ndarray]:
    """
    Replace inf values with nan values in a numpy array
    """
    inf_mask = np.isinf(arr)
    if np.sum(inf_mask) != 0:
        arr_org = arr.copy()
        arr[inf_mask] = np.nan
        return Some(arr_org)
    else:
        return Nothing
