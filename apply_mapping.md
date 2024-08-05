```python

from typing import Callable, Any, Tuple

def validate_args(*validators: Callable[[Any], None]):
    """
    A decorator that applies a series of validation functions to the arguments of the decorated function.

    This decorator allows you to specify validation functions for each argument of the decorated function.
    It will apply these validators in order to the positional arguments, and to keyword arguments if they
    match the parameter names of the decorated function.

    Args:
        *validators (Callable[[Any], None]): A variable number of validation functions. Each function
            should take a single argument and raise an exception if the validation fails.

    Returns:
        Callable: A decorator function that can be applied to other functions.

    Raises:
        ValueError: If a validation function raises an exception, it's caught and re-raised as a ValueError
            with additional context about which argument failed validation.

    Examples:
        Define some validation functions:

        >>> def validate_positive(value):
        ...     if value <= 0:
        ...         raise ValueError("Value must be positive")
        
        >>> def validate_string(value):
        ...     if not isinstance(value, str):
        ...         raise TypeError("Value must be a string")

        Use the decorator with positional arguments:

        >>> @validate_args(validate_positive, validate_positive, validate_string)
        ... def create_rectangle(width, height, color):
        ...     return f"A {color} rectangle of size {width}x{height}"
        
        >>> create_rectangle(5, 10, "blue")
        'A blue rectangle of size 5x10'
        
        >>> create_rectangle(-5, 10, "red")
        Traceback (most recent call last):
            ...
        ValueError: Validation failed for argument: -5. Error: Value must be positive

        Use the decorator with keyword arguments:

        >>> @validate_args(validate_string, validate_positive)
        ... def greet(name: str, times: int):
        ...     return f"Hello, {name}! " * times
        
        >>> greet(name="Alice", times=3)
        'Hello, Alice! Hello, Alice! Hello, Alice! '
        
        >>> greet(name=123, times=2)
        Traceback (most recent call last):
            ...
        ValueError: Validation failed for argument 'name': 123. Error: Value must be a string

    Note:
        - The number of validators should match the number of parameters in the decorated function.
        - If a validator is not needed for a particular parameter, you can use `lambda x: None` as a no-op validator.
        - The decorator preserves the original function's metadata (e.g., name, docstring) using `functools.wraps`.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        from functools import wraps
        
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Validate positional arguments
            for validator, arg in zip(validators, args):
                try:
                    validator(arg)
                except Exception as e:
                    raise ValueError(f"Validation failed for argument: {arg}. Error: {str(e)}")
            
            # Validate keyword arguments
            func_params = func.__annotations__
            for param_name, param_value in kwargs.items():
                if param_name in func_params:
                    validator_index = list(func_params.keys()).index(param_name)
                    if validator_index < len(validators):
                        try:
                            validators[validator_index](param_value)
                        except Exception as e:
                            raise ValueError(f"Validation failed for argument '{param_name}': {param_value}. Error: {str(e)}")
            
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator

```

