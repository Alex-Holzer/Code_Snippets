To ensure that a large language model (LLM) outputs **only code** without explanations or additional text, you can modify the prompt to be more direct and explicit about this requirement. The prompt below is refined to focus strictly on code generation based on the guidelines and problem context you provide.

### Optimized Prompt for Code-Only Output:

**Objective:** Generate Python and PySpark code solutions that maximize efficiency, maintainability, and adhere to best practices. The code must be optimized for large-scale data processing in distributed computing environments. Do not include any explanations or additional text—only the code.

#### Code Generation Guidelines:
1. **Efficiency Optimization**  
    - Utilize algorithms and data structures that minimize computational complexity and execution time. Ensure suitability for PySpark’s distributed architecture.  
    - Apply PySpark-specific optimizations like broadcast variables, partitioning strategies, and caching mechanisms.

2. **Style Guide Adherence**  
    - Strictly follow PEP8 for readability and consistency.  
    - Implement PySpark best practices, minimizing data shuffles and selecting optimal transformations.

3. **Type Annotations and Refactoring**  
    - Add comprehensive type hints in all functions and methods.  
    - Refactor complex or inefficient code segments for readability and maintainability without changing functionality.

4. **Comprehensive Documentation**  
    - Use the `numpydoc` style for docstrings, detailing function purpose, parameters, returns, and exceptions.

5. **Seamless Code Integration**  
    - Build on previous code snippets to maintain continuity and avoid unnecessary repetition.  
    - Ensure code can be easily integrated into larger systems without loss of context.

6. **Practical Example**  
    - Provide real-world code examples, particularly for large-scale data processing tasks with PySpark.

#### Example Problem:  
Develop a PySpark application to process and analyze large-scale user activity logs to identify peak usage times.

#### Requirements:
- Optimize for distributed processing.  
- Ensure minimal data shuffling.  
- Follow PEP8 and PySpark best practices.

#### Expected Output:
- Provide refactored PySpark code with type hints.  
- Include comprehensive docstrings using numpydoc style.

---

By making the intent of "code-only" output explicit and removing any requests for explanations or additional details, this prompt will encourage the LLM to focus purely on generating the required code snippets.

