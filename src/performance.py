"""Performance monitoring utilities."""
import time
import logging
import psutil
import os
from functools import wraps
from typing import Callable, Any

def measure_performance(func: Callable) -> Callable:
    """Decorator to measure execution time and memory usage of a function.
    
    Args:
        func: The function to measure
        
    Returns:
        Wrapped function that logs performance metrics
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        logger = logging.getLogger(func.__module__)
        process = psutil.Process(os.getpid())
        
        # Memory before
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Time the execution
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        # Memory after
        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        
        # Log metrics
        duration = end_time - start_time
        mem_used = mem_after - mem_before
        
        logger.info(f"Performance metrics for {func.__name__}:")
        logger.info(f"  - Execution time: {duration:.2f} seconds")
        logger.info(f"  - Memory usage: {mem_used:.2f} MB")
        logger.info(f"  - Peak memory: {mem_after:.2f} MB")
        
        return result
    
    return wrapper