import numpy as np
from userzizi_timing import measure_time

# Python list
numbers_list = [1, 2, 3, 4, 5]

# NumPy array
numbers_array = np.array([1, 2, 3, 4, 5])

print("List:", numbers_list)
print("NumPy Array:", numbers_array)

@measure_time
def multiply_with_list(lst, scalar):
    return [x * scalar for x in lst]

@measure_time
def multiply_with_numpy(arr, scalar):
    return arr * scalar

# Run performance comparison
multiply_with_list(numbers_list, 5)
multiply_with_numpy(numbers_array, 5)