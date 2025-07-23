"""Tests for method docstring validation (instance, static, and class methods).

This test suite ensures that the docstring validator correctly handles
different types of methods within classes, testing both valid and invalid
docstrings for comprehensive coverage.
"""

import pytest
from automation.docstring_lint.validator import DocstringValidator


class TestInstanceMethodDocstrings:
    """Test validation of instance method docstrings."""

    @pytest.fixture
    def validator(self):
        """Provide a DocstringValidator instance for tests."""
        return DocstringValidator()

    def test_valid_instance_method_docstring(self, validator):
        """Test valid instance method with proper Google-style docstring."""
        docstring = '''"""Process user data with validation.

        This method validates and processes user input data according to
        business rules and returns the processed result.

        Args:
            user_data: Dictionary containing user information
            validate_email: Whether to validate email format
            timeout: Maximum time to wait for processing in seconds

        Returns:
            Processed user data dictionary with validated fields

        Raises:
            ValueError: If user data is invalid or missing required fields
            TimeoutError: If processing takes longer than timeout

        Examples:
            >>> processor = DataProcessor()
            >>> data = {"name": "John", "email": "john@example.com"}
            >>> result = processor.process_user_data(data, validate_email=True)
            >>> result["name"]
            'John'
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.process_user_data")

        assert result.is_valid()
        assert not result.has_errors()
        # May have minor warnings which is OK

    def test_minimal_valid_instance_method_docstring(self, validator):
        """Test minimal but valid instance method docstring."""
        docstring = '''"""Calculate user score based on activity.

        Args:
            activity_data: User activity information

        Returns:
            Numerical score representing user engagement
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.calculate_score")

        assert result.is_valid()
        assert not result.has_errors()

    def test_instance_method_with_self_in_args(self, validator):
        """Test instance method incorrectly documenting 'self' parameter."""
        docstring = '''"""Process data for the instance.

        Args:
            self: The instance itself (WRONG - should not document self)
            data: Data to process

        Returns:
            Processed data
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.process_data")

        # Should still be valid RST, but this is a style issue
        # The linter doesn't check for self documentation specifically
        assert result.parsing_successful

    def test_instance_method_missing_return_doc(self, validator):
        """Test instance method missing Returns section when it should have one."""
        docstring = '''"""Calculate total price including tax.

        This method calculates the total price by adding tax to base price.

        Args:
            base_price: The base price before tax
            tax_rate: Tax rate as decimal (e.g., 0.08 for 8%)
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.calculate_total")

        # RST-wise this is valid, just missing documentation
        assert result.parsing_successful

    def test_instance_method_malformed_args_section(self, validator):
        """Test instance method with malformed Args section."""
        docstring = '''"""Update user preferences.

        arguments:  # Should be "Args:"
            user_id: ID of the user
            preferences: New preference settings

        Returns:
            Updated user object
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.update_preferences")

        # Should detect malformed section header
        assert result.has_warnings() or result.has_errors()
        messages = " ".join(result.warnings + result.errors).lower()
        assert "malformed section header" in messages or "rst syntax" in messages

    def test_instance_method_incorrect_indentation(self, validator):
        """Test instance method with incorrect parameter indentation."""
        docstring = '''"""Validate user input data.

        Args:
        user_data: Data to validate (not indented properly)
        rules: Validation rules to apply (also not indented)

        Returns:
            True if valid, False otherwise
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.validate_data")

        # Should detect indentation issues
        assert result.has_warnings() or result.has_errors()

    def test_instance_method_unmatched_backticks(self, validator):
        """Test instance method with unmatched backticks in docstring."""
        docstring = '''"""Format data for display.

        This method formats data using `special formatting rules and
        converts it to a `user-friendly format.

        Args:
            data: Raw data to format
            format_type: Type of formatting to apply

        Returns:
            Formatted data string with ``unmatched double backticks
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.format_data")

        # Should detect unmatched backticks
        assert result.has_warnings() or result.has_errors()

    def test_instance_method_malformed_code_block(self, validator):
        """Test instance method with malformed code block."""
        docstring = '''"""Execute database query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Query results

        Examples:
            .. code-block: python  # Missing double colon

                db = Database()
                results = db.execute_query("SELECT * FROM users", {})
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.execute_query")

        # Should detect malformed code block
        assert result.has_warnings() or result.has_errors()

    def test_instance_method_empty_docstring(self, validator):
        """Test instance method with empty docstring."""
        result = validator.validate_docstring_text("", "TestClass.empty_method")

        assert result.has_warnings()
        assert "No docstring found" in result.warnings[0]
        assert result.is_valid()  # Empty is valid, just warned

    def test_instance_method_whitespace_only_docstring(self, validator):
        """Test instance method with whitespace-only docstring."""
        result = validator.validate_docstring_text("   \n\t  \n   ", "TestClass.whitespace_method")

        assert result.has_warnings()
        assert "No docstring found" in result.warnings[0]

    def test_instance_method_complex_valid_docstring(self, validator):
        """Test instance method with complex but valid docstring."""
        docstring = '''"""Analyze user behavior patterns with advanced metrics.

        This method performs comprehensive analysis of user behavior data
        using machine learning algorithms and statistical methods to identify
        patterns and trends.

        Args:
            user_data: Dictionary containing user interaction data with keys:
                - 'clicks': List of click timestamps
                - 'views': List of page view data
                - 'sessions': Session duration information
            analysis_config: Configuration object specifying:
                - algorithms to use
                - statistical thresholds
                - output format preferences
            include_predictions: Whether to include future behavior predictions
            time_range: Tuple of (start_date, end_date) for analysis period

        Returns:
            AnalysisResult object containing:
                - pattern_summary: Dictionary of identified patterns
                - metrics: Statistical measures and confidence intervals
                - predictions: Future behavior predictions (if requested)
                - visualization_data: Data formatted for chart generation

        Raises:
            InvalidDataError: If user_data is malformed or missing required keys
            ConfigurationError: If analysis_config contains invalid parameters
            InsufficientDataError: If not enough data for meaningful analysis
            TimeoutError: If analysis takes longer than configured timeout

        Examples:
            Basic usage with default configuration:

            >>> analyzer = BehaviorAnalyzer()
            >>> data = {
            ...     'clicks': [timestamp1, timestamp2],
            ...     'views': [view_data1, view_data2],
            ...     'sessions': [session1, session2]
            ... }
            >>> config = AnalysisConfig(algorithms=['clustering', 'regression'])
            >>> result = analyzer.analyze_patterns(data, config)
            >>> print(result.pattern_summary)
            {'frequent_patterns': [...], 'anomalies': [...]}

            Advanced usage with predictions:

            >>> result = analyzer.analyze_patterns(
            ...     data, config, 
            ...     include_predictions=True,
            ...     time_range=(date(2023, 1, 1), date(2023, 12, 31))
            ... )
            >>> result.predictions['next_month_engagement']
            0.85

        Note:
            This method requires substantial computational resources for large
            datasets. Consider using ``analyze_patterns_batch`` for datasets
            larger than 100,000 user records.

        See Also:
            analyze_patterns_batch: Batch processing version
            simple_behavior_analysis: Lightweight alternative
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.analyze_patterns")

        # Should be valid despite complexity
        assert result.is_valid()
        # May have some warnings which is acceptable for complex docstrings


class TestStaticMethodDocstrings:
    """Test validation of static method docstrings."""

    @pytest.fixture
    def validator(self):
        """Provide a DocstringValidator instance for tests."""
        return DocstringValidator()

    def test_valid_static_method_docstring(self, validator):
        """Test valid static method with proper Google-style docstring."""
        docstring = '''"""Validate email address format using regex.

        This static method validates email format without requiring
        class instance state.

        Args:
            email: Email address string to validate
            strict: Whether to use strict validation rules

        Returns:
            True if email format is valid, False otherwise

        Examples:
            >>> UserValidator.validate_email("user@example.com")
            True
            >>> UserValidator.validate_email("invalid-email")
            False
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.validate_email")

        assert result.is_valid()
        assert not result.has_errors()

    def test_static_method_utility_function_style(self, validator):
        """Test static method documented as utility function."""
        docstring = '''"""Calculate distance between two geographic points.

        Uses the Haversine formula to calculate the great circle distance
        between two points on Earth given their latitude and longitude.

        Args:
            lat1: Latitude of first point in decimal degrees
            lon1: Longitude of first point in decimal degrees
            lat2: Latitude of second point in decimal degrees
            lon2: Longitude of second point in decimal degrees
            unit: Unit for result ('km', 'mi', or 'nm')

        Returns:
            Distance between points in specified unit

        Raises:
            ValueError: If coordinates are invalid or unit not recognized

        Examples:
            Calculate distance between New York and London:

            >>> dist = GeoUtils.calculate_distance(
            ...     40.7128, -74.0060,  # New York
            ...     51.5074, -0.1278,   # London
            ...     unit='km'
            ... )
            >>> round(dist)
            5585
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.calculate_distance")

        assert result.is_valid()
        assert not result.has_errors()

    def test_static_method_with_class_references(self, validator):
        """Test static method that references class but doesn't need instance."""
        docstring = '''"""Create default configuration for DataProcessor instances.

        This static method provides default configuration values that can
        be used when creating new DataProcessor instances.

        Args:
            environment: Target environment ('dev', 'staging', 'prod')
            feature_flags: Optional feature flags to enable

        Returns:
            ProcessorConfig object with default values for the environment

        Examples:
            >>> config = DataProcessor.create_default_config('prod')
            >>> processor = DataProcessor(config)
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.create_default_config")

        assert result.is_valid()
        assert not result.has_errors()

    def test_static_method_malformed_section_header(self, validator):
        """Test static method with malformed section headers."""
        docstring = '''"""Parse configuration from string format.

        parameters:  # Should be "Args:"
            config_string: String containing configuration data
            format_type: Type of format ('json', 'yaml', 'ini')

        return:  # Should be "Returns:"
            Parsed configuration dictionary
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.parse_config")

        # Should detect malformed section headers
        assert result.has_warnings() or result.has_errors()
        messages = " ".join(result.warnings + result.errors).lower()
        assert "malformed section header" in messages or "rst syntax" in messages

    def test_static_method_missing_args_indentation(self, validator):
        """Test static method with missing parameter indentation."""
        docstring = '''"""Convert string to standardized format.

        Args:
        input_string: String to convert
        case_style: Desired case style ('snake', 'camel', 'pascal')
        remove_special: Whether to remove special characters

        Returns:
            Converted string in specified format
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.format_string")

        # Should detect indentation issues
        assert result.has_warnings() or result.has_errors()

    def test_static_method_with_rst_syntax_errors(self, validator):
        """Test static method with RST syntax errors."""
        docstring = '''"""Generate hash for given input data.

        This method creates a secure hash using `specified algorithm
        and returns the result as a `hexadecimal string.

        Args:
            data: Data to hash
            algorithm: Hash algorithm to use

        Returns:
            Hash value as hexadecimal string with ``unmatched backticks

        Examples:
            .. code-block: python  # Missing double colon

                hash_val = HashUtils.generate_hash("test", "sha256")
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.generate_hash")

        # Should detect multiple RST syntax errors
        assert result.has_warnings() or result.has_errors()

    def test_static_method_empty_sections(self, validator):
        """Test static method with empty sections."""
        docstring = '''"""Check if value meets criteria.

        Args:
            # Empty args section

        Returns:
            # Empty returns section

        Raises:
            # Empty raises section
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.check_criteria")

        # Should detect empty sections
        assert result.has_warnings() or result.has_errors()

    def test_static_method_no_parameters(self, validator):
        """Test static method with no parameters (valid case)."""
        docstring = '''"""Get current timestamp in ISO format.

        Returns:
            Current timestamp as ISO formatted string

        Examples:
            >>> timestamp = TimeUtils.get_iso_timestamp()
            >>> len(timestamp)
            19
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.get_iso_timestamp")

        assert result.is_valid()
        assert not result.has_errors()


class TestClassMethodDocstrings:
    """Test validation of class method docstrings."""

    @pytest.fixture
    def validator(self):
        """Provide a DocstringValidator instance for tests."""
        return DocstringValidator()

    def test_valid_class_method_docstring(self, validator):
        """Test valid class method with proper Google-style docstring."""
        docstring = '''"""Create instance from configuration file.

        This class method provides an alternative constructor that reads
        configuration from a file and creates a new instance.

        Args:
            config_file: Path to configuration file
            validate_config: Whether to validate configuration before creation
            default_values: Optional default values for missing config items

        Returns:
            New instance of the class configured from file

        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ConfigurationError: If configuration is invalid
            PermissionError: If unable to read configuration file

        Examples:
            >>> processor = DataProcessor.from_config('config.yaml')
            >>> processor.config['timeout']
            30

            With validation disabled:

            >>> processor = DataProcessor.from_config(
            ...     'config.yaml', 
            ...     validate_config=False
            ... )
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.from_config")

        assert result.is_valid()
        assert not result.has_errors()

    def test_class_method_factory_pattern(self, validator):
        """Test class method used as factory constructor."""
        docstring = '''"""Create database connection from URL.

        Factory method that parses a database URL and creates an
        appropriately configured database connection instance.

        Args:
            db_url: Database connection URL (e.g., 'postgresql://user:pass@host/db')
            pool_size: Maximum connection pool size
            timeout: Connection timeout in seconds

        Returns:
            Database connection instance configured for the specified database

        Raises:
            ValueError: If URL format is invalid
            ConnectionError: If unable to connect to database

        Examples:
            >>> db = Database.from_url('postgresql://user:pass@localhost/mydb')
            >>> db.execute('SELECT 1')
            [(1,)]
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.from_url")

        assert result.is_valid()
        assert not result.has_errors()

    def test_class_method_with_cls_parameter(self, validator):
        """Test class method incorrectly documenting 'cls' parameter."""
        docstring = '''"""Create instance with default settings.

        Args:
            cls: The class itself (WRONG - should not document cls)
            name: Instance name
            settings: Configuration settings

        Returns:
            New instance with specified settings
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.with_defaults")

        # Should still be valid RST, but this is a style issue
        # The linter doesn't check for cls documentation specifically
        assert result.parsing_successful

    def test_class_method_inherits_behavior(self, validator):
        """Test class method that references inheritance behavior."""
        docstring = '''"""Create instance using inherited class behavior.

        This class method leverages the class hierarchy to create an
        instance with behavior appropriate for the specific subclass.

        Args:
            config_data: Configuration data dictionary
            inherit_parent_config: Whether to inherit parent class configuration

        Returns:
            Instance of the appropriate subclass based on config_data

        Note:
            The actual class of the returned instance depends on the class
            this method is called on and the configuration provided.

        Examples:
            >>> # Called on parent class
            >>> instance = BaseProcessor.create_configured({'type': 'text'})
            >>> type(instance).__name__
            'TextProcessor'

            >>> # Called on subclass directly
            >>> instance = TextProcessor.create_configured({'advanced': True})
            >>> isinstance(instance, TextProcessor)
            True
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.create_configured")

        assert result.is_valid()
        assert not result.has_errors()

    def test_class_method_malformed_returns_section(self, validator):
        """Test class method with malformed Returns section."""
        docstring = '''"""Load configuration from multiple sources.

        Args:
            sources: List of configuration sources to load
            merge_strategy: How to merge conflicting values

        Return:  # Should be "Returns:"
            Merged configuration dictionary
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.load_config")

        # Should detect malformed section header
        assert result.has_warnings() or result.has_errors()
        messages = " ".join(result.warnings + result.errors).lower()
        assert "malformed section header" in messages or "rst syntax" in messages

    def test_class_method_parameter_formatting_errors(self, validator):
        """Test class method with parameter formatting errors."""
        docstring = '''"""Initialize from environment variables.

        Args:
            prefix (str: Missing closing parenthesis for type annotation
            required_vars list): Wrong parenthesis placement
            default_config (dict)): Extra closing parenthesis

        Returns:
            Configured class instance
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.from_env")

        # Should detect formatting issues with parentheses
        assert result.has_warnings() or result.has_errors()

    def test_class_method_code_block_errors(self, validator):
        """Test class method with code block formatting errors."""
        docstring = '''"""Create instance with custom serialization.

        Args:
            serializer_type: Type of serializer to use
            custom_fields: Fields requiring custom serialization

        Returns:
            Instance configured with specified serializer

        Examples:
            .. code-block: python  # Missing double colon
                
                serializer = CustomSerializer.create_with_serialization(
                    'json', ['date_field', 'complex_object']
                )

            Another example with wrong directive:
            
            .. code-kjfdblock:: python  # Misspelled directive
            
                # This won't render properly
                result = serializer.serialize(data)
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.create_with_serialization")

        # Should detect code block directive errors
        assert result.has_warnings() or result.has_errors()

    def test_class_method_unmatched_markup(self, validator):
        """Test class method with unmatched markup elements."""
        docstring = '''"""Build instance from template configuration.

        This method creates an instance using `template-based configuration
        and applies `custom modifications as needed.

        Args:
            template_name: Name of configuration template
            modifications: Dictionary of ``template modifications

        Returns:
            Instance built from `modified template configuration

        Examples:
            Use a predefined template:

            >>> builder = ConfigBuilder.from_template('web_server')
            >>> builder.config['port']
            8080
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.from_template")

        # Should detect unmatched backticks
        assert result.has_warnings() or result.has_errors()

    def test_class_method_complex_inheritance_example(self, validator):
        """Test complex class method with inheritance documentation."""
        docstring = '''"""Create specialized instance based on runtime requirements.

        This advanced factory method analyzes runtime requirements and
        dynamically selects the most appropriate subclass implementation.
        It handles complex inheritance hierarchies and plugin architectures.

        Args:
            requirements: Requirements specification containing:
                - 'performance_tier': Performance requirements ('low', 'medium', 'high')
                - 'feature_set': Required features list
                - 'compatibility_mode': Backward compatibility requirements
                - 'resource_constraints': Memory and CPU limitations
            plugin_registry: Optional plugin registry for extended functionality
            fallback_class: Class to use if no suitable implementation found
            validation_level: Level of requirement validation ('strict', 'lenient', 'off')

        Returns:
            Specialized instance optimized for the given requirements.
            The actual class depends on requirement analysis:

            - HighPerformanceProcessor: For performance_tier='high'
            - CompatibilityProcessor: For legacy compatibility_mode
            - PluginProcessor: When plugin_registry contains relevant plugins
            - BaseProcessor: As fallback when no specialized class matches

        Raises:
            RequirementsError: If requirements are contradictory or impossible
            PluginError: If required plugins are unavailable or incompatible
            ResourceError: If resource constraints cannot be satisfied
            ValidationError: If validation_level='strict' and requirements invalid

        Examples:
            Create high-performance instance:

            >>> reqs = {
            ...     'performance_tier': 'high',
            ...     'feature_set': ['caching', 'parallel_processing'],
            ...     'resource_constraints': {'max_memory': '2GB'}
            ... }
            >>> processor = ProcessorFactory.create_specialized(reqs)
            >>> processor.__class__.__name__
            'HighPerformanceProcessor'

            Create with plugin support:

            >>> registry = PluginRegistry(['analytics', 'monitoring'])
            >>> processor = ProcessorFactory.create_specialized(
            ...     {'feature_set': ['analytics']}, 
            ...     plugin_registry=registry
            ... )
            >>> hasattr(processor, 'analytics_plugin')
            True

            Handle fallback scenarios:

            >>> impossible_reqs = {'performance_tier': 'impossible'}
            >>> processor = ProcessorFactory.create_specialized(
            ...     impossible_reqs,
            ...     fallback_class=BasicProcessor,
            ...     validation_level='lenient'
            ... )
            >>> isinstance(processor, BasicProcessor)
            True

        Note:
            This method performs expensive analysis of the class hierarchy
            and requirement compatibility. Consider caching results for
            frequently used requirement patterns.

            The method supports plugin architectures through the plugin_registry
            parameter. Plugins are loaded dynamically and integrated into the
            selected processor class.

        See Also:
            create_basic: Simple factory method for basic requirements
            PluginRegistry: Documentation for plugin system
            ProcessorCapabilities: Available processor types and features
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.create_specialized")

        # Should be valid despite complexity
        assert result.is_valid()
        # Complex docstrings may have warnings but should parse successfully


class TestMethodDocstringEdgeCases:
    """Test edge cases and unusual scenarios for method docstrings."""

    @pytest.fixture
    def validator(self):
        """Provide a DocstringValidator instance for tests."""
        return DocstringValidator()

    def test_method_with_only_triple_quotes(self, validator):
        """Test method with docstring containing only triple quotes."""
        result = validator.validate_docstring_text('""""""', "TestClass.empty_quotes_method")

        # Empty string should trigger "No docstring found" warning
        assert result.has_warnings() or result.has_errors()
        # The validator may interpret this differently, but it should have issues

    def test_method_with_only_whitespace_in_quotes(self, validator):
        """Test method with docstring containing only whitespace."""
        docstring = '''"""   
        
        
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.whitespace_quotes_method")

        # Whitespace-only docstrings may be treated as RST content
        # Should have warnings or errors due to lack of meaningful content
        assert result.has_warnings() or result.has_errors()

    def test_method_single_word_docstring(self, validator):
        """Test method with single word docstring."""
        result = validator.validate_docstring_text('"""Helper."""', "TestClass.helper_method")

        assert result.is_valid()
        assert not result.has_errors()

    def test_method_with_unicode_content(self, validator):
        """Test method docstring with unicode content."""
        docstring = '''"""Process unicode data with special characters.

        This method handles unicode strings containing emojis, accented
        characters, and other international text properly.

        Args:
            text: Unicode text string (e.g., "Héllo 🌍 Wørld!")
            encoding: Target encoding for output

        Returns:
            Processed unicode string with normalized encoding

        Examples:
            >>> process_unicode("Café ☕", "utf-8")
            'Café ☕'
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.process_unicode")

        assert result.is_valid()
        assert not result.has_errors()

    def test_method_extremely_long_parameter_description(self, validator):
        """Test method with extremely long parameter descriptions."""
        docstring = '''"""Process complex configuration data.

        Args:
            config: A very complex configuration object that contains numerous nested
                   dictionaries, lists, and other data structures representing the
                   complete application configuration including database connection
                   parameters, API endpoint definitions, logging configuration,
                   security settings, feature flags, environment-specific overrides,
                   caching configuration, monitoring and metrics settings, plugin
                   configuration, theme and UI customization options, user preference
                   defaults, system resource limits, backup and recovery settings,
                   and many other aspects of the application that need to be
                   configured for proper operation in various deployment environments

        Returns:
            Processed configuration ready for application use
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.process_complex_config")

        assert result.is_valid()
        # Long descriptions are fine from RST perspective

    def test_method_with_nested_sections_and_sublists(self, validator):
        """Test method with complex nested documentation structure."""
        docstring = '''"""Execute multi-stage data processing pipeline.

        Args:
            pipeline_config: Configuration for the processing pipeline:

                - stage_1: Initial data ingestion configuration:

                    - sources: List of data sources
                    - formats: Supported input formats ['csv', 'json', 'xml']
                    - validation_rules: Data validation configuration

                - stage_2: Data transformation configuration:

                    - transformers: List of transformation operations
                    - output_format: Desired output format
                    - error_handling: How to handle transformation errors

                - stage_3: Data export configuration:

                    - destinations: Export target configurations
                    - batch_size: Number of records per batch
                    - retry_policy: Retry configuration for failed exports

            execution_mode: Execution mode selection:

                - 'sequential': Process stages one after another
                - 'parallel': Process compatible stages in parallel
                - 'adaptive': Automatically choose based on data size

        Returns:
            PipelineResult containing:

                - execution_summary: High-level execution statistics
                - stage_results: Detailed results for each stage
                - performance_metrics: Timing and resource usage data
                - error_log: Any errors encountered during processing

        Raises:
            PipelineConfigError: If configuration is invalid
            DataProcessingError: If processing fails at any stage
            ResourceError: If insufficient system resources
        """'''
        result = validator.validate_docstring_text(docstring, "TestClass.execute_pipeline")

        # Complex nested structure should still be valid RST
        assert result.parsing_successful
        # May have some warnings about formatting which is OK
