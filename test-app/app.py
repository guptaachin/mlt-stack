"""
Test Application demonstrating OpenTelemetry correlation
- Traces: Spans for each request and operation
- Logs: JSON logs with traceID/spanID injected
- Metrics: Request counters and duration histograms
"""

import logging
import json
import time
import random
import threading
from flask import Flask, request, jsonify

# OpenTelemetry imports
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.flask import FlaskInstrumentor

# ============================================================================
# SETUP: Resource (identifies this service)
# ============================================================================
resource = Resource.create({
    "service.name": "test-app",
    "service.version": "1.0.0",
    "deployment.environment": "development"
})

OTEL_ENDPOINT = "otel-collector:4317"

# ============================================================================
# SETUP: Tracing
# ============================================================================
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    )
)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# ============================================================================
# SETUP: Metrics
# ============================================================================
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True),
    export_interval_millis=5000
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Create metrics
request_counter = meter.create_counter(
    name="http_requests_total",
    description="Total HTTP requests",
    unit="1"
)

request_duration = meter.create_histogram(
    name="http_request_duration_seconds",
    description="HTTP request duration in seconds",
    unit="s"
)

active_requests = meter.create_up_down_counter(
    name="http_requests_active",
    description="Number of active HTTP requests",
    unit="1"
)

# ============================================================================
# SETUP: Logging with Trace Context
# ============================================================================
logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(
    BatchLogRecordProcessor(
        OTLPLogExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    )
)

# Custom formatter that adds trace context to console output
class TraceContextFormatter(logging.Formatter):
    def format(self, record):
        span = trace.get_current_span()
        ctx = span.get_span_context()
        
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add trace context if we're in an active span
        if ctx.is_valid:
            log_entry["traceID"] = format(ctx.trace_id, '032x')
            log_entry["spanID"] = format(ctx.span_id, '016x')
        
        # Add any extra fields
        if hasattr(record, 'extra_data'):
            log_entry.update(record.extra_data)
            
        return json.dumps(log_entry)

# Setup logging
logger = logging.getLogger("test-app")
logger.setLevel(logging.INFO)

# Console handler with trace context (picked up by fluentd)
console_handler = logging.StreamHandler()
console_handler.setFormatter(TraceContextFormatter())
logger.addHandler(console_handler)

# OTLP handler (sends logs directly with trace context)
otlp_handler = LoggingHandler(logger_provider=logger_provider)
logger.addHandler(otlp_handler)

# ============================================================================
# Flask App
# ============================================================================
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)


def log_with_context(message, level="info", **extra):
    """Helper to log with extra context"""
    record = logging.LogRecord(
        name="test-app",
        level=getattr(logging, level.upper()),
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None
    )
    record.extra_data = extra
    logger.handle(record)


@app.route("/")
def home():
    """Simple endpoint - generates trace + log + metric"""
    start_time = time.time()
    active_requests.add(1)
    
    try:
        log_with_context("Home page requested", endpoint="/")
        
        # Simulate some work
        time.sleep(random.uniform(0.01, 0.05))
        
        request_counter.add(1, {"endpoint": "/", "method": "GET", "status": "200"})
        
        return jsonify({
            "message": "Hello from test-app!",
            "tip": "Check Grafana to see traces, logs, and metrics"
        })
    finally:
        active_requests.add(-1)
        duration = time.time() - start_time
        request_duration.record(duration, {"endpoint": "/", "method": "GET"})


@app.route("/api/orders", methods=["POST"])
def create_order():
    """
    Complex endpoint demonstrating nested spans with correlated logs.
    Each step creates a child span, and logs include the trace context.
    """
    start_time = time.time()
    active_requests.add(1)
    
    try:
        order_id = f"ORD-{random.randint(1000, 9999)}"
        
        log_with_context("Order creation started", order_id=order_id)
        
        # Step 1: Validate order (child span)
        with tracer.start_as_current_span("validate-order") as span:
            span.set_attribute("order.id", order_id)
            log_with_context("Validating order", step="validation", order_id=order_id)
            time.sleep(random.uniform(0.02, 0.08))
            
            # Add span event (always visible in trace)
            span.add_event("validation_complete", {"status": "passed"})
        
        # Step 2: Check inventory (child span)
        with tracer.start_as_current_span("check-inventory") as span:
            span.set_attribute("order.id", order_id)
            items_count = random.randint(1, 5)
            span.set_attribute("items.count", items_count)
            
            log_with_context("Checking inventory", step="inventory", order_id=order_id, items=items_count)
            time.sleep(random.uniform(0.03, 0.1))
            
            # Nested span for database query
            with tracer.start_as_current_span("db-query") as db_span:
                db_span.set_attribute("db.system", "postgresql")
                db_span.set_attribute("db.operation", "SELECT")
                log_with_context("Executing inventory query", step="db_query", order_id=order_id)
                time.sleep(random.uniform(0.01, 0.03))
                db_span.add_event("query_executed", {"rows_returned": items_count})
        
        # Step 3: Process payment (child span)
        with tracer.start_as_current_span("process-payment") as span:
            amount = random.uniform(10.0, 500.0)
            span.set_attribute("payment.amount", amount)
            span.set_attribute("payment.currency", "USD")
            
            log_with_context("Processing payment", step="payment", order_id=order_id, amount=f"${amount:.2f}")
            time.sleep(random.uniform(0.05, 0.15))
            
            # Simulate occasional slow payment
            if random.random() < 0.1:
                log_with_context("Payment gateway slow", level="warning", step="payment", order_id=order_id)
                time.sleep(0.2)
            
            span.add_event("payment_processed", {"transaction_id": f"TXN-{random.randint(10000, 99999)}"})
        
        log_with_context("Order created successfully", order_id=order_id, status="completed")
        
        request_counter.add(1, {"endpoint": "/api/orders", "method": "POST", "status": "201"})
        
        return jsonify({
            "order_id": order_id,
            "status": "created",
            "message": "Order processed successfully"
        }), 201
        
    except Exception as e:
        log_with_context("Order creation failed", level="error", error=str(e))
        request_counter.add(1, {"endpoint": "/api/orders", "method": "POST", "status": "500"})
        return jsonify({"error": str(e)}), 500
        
    finally:
        active_requests.add(-1)
        duration = time.time() - start_time
        request_duration.record(duration, {"endpoint": "/api/orders", "method": "POST"})


@app.route("/api/orders/<order_id>")
def get_order(order_id):
    """Get order details - simpler example with one child span"""
    start_time = time.time()
    active_requests.add(1)
    
    try:
        log_with_context("Fetching order", order_id=order_id)
        
        with tracer.start_as_current_span("fetch-from-db") as span:
            span.set_attribute("db.system", "postgresql")
            span.set_attribute("order.id", order_id)
            
            log_with_context("Querying database", order_id=order_id, operation="SELECT")
            time.sleep(random.uniform(0.02, 0.06))
            
            # Simulate not found sometimes
            if random.random() < 0.2:
                span.set_attribute("order.found", False)
                log_with_context("Order not found", level="warning", order_id=order_id)
                request_counter.add(1, {"endpoint": "/api/orders/{id}", "method": "GET", "status": "404"})
                return jsonify({"error": "Order not found"}), 404
            
            span.set_attribute("order.found", True)
        
        log_with_context("Order retrieved", order_id=order_id)
        request_counter.add(1, {"endpoint": "/api/orders/{id}", "method": "GET", "status": "200"})
        
        return jsonify({
            "order_id": order_id,
            "status": "completed",
            "items": random.randint(1, 5),
            "total": f"${random.uniform(10, 500):.2f}"
        })
        
    finally:
        active_requests.add(-1)
        duration = time.time() - start_time
        request_duration.record(duration, {"endpoint": "/api/orders/{id}", "method": "GET"})


@app.route("/api/error")
def trigger_error():
    """Endpoint to demonstrate error tracing and logging"""
    start_time = time.time()
    active_requests.add(1)
    
    try:
        log_with_context("Error endpoint called - this will fail", level="warning")
        
        with tracer.start_as_current_span("failing-operation") as span:
            span.set_attribute("error", True)
            log_with_context("About to raise exception", level="error")
            
            # Record exception in span
            try:
                raise ValueError("Simulated error for demonstration")
            except ValueError as e:
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                log_with_context("Exception occurred", level="error", error=str(e), error_type="ValueError")
                raise
                
    except ValueError as e:
        request_counter.add(1, {"endpoint": "/api/error", "method": "GET", "status": "500"})
        return jsonify({"error": str(e)}), 500
        
    finally:
        active_requests.add(-1)
        duration = time.time() - start_time
        request_duration.record(duration, {"endpoint": "/api/error", "method": "GET"})


@app.route("/health")
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})


# ============================================================================
# Background MLT Emitter - Continuously generates traces, logs, and metrics
# ============================================================================
def background_mlt_emitter():
    """
    Background thread that continuously emits correlated MLT data.
    This simulates real application activity for demonstration purposes.
    """
    operations = [
        ("user-login", ["validate-credentials", "create-session", "log-audit"]),
        ("data-sync", ["fetch-remote", "transform-data", "store-local"]),
        ("report-generation", ["query-data", "aggregate-results", "format-output"]),
        ("cache-refresh", ["check-staleness", "fetch-new-data", "update-cache"]),
        ("health-check", ["check-db", "check-cache", "check-external-api"]),
    ]
    
    # Background-specific metrics
    bg_operations = meter.create_counter(
        name="background_operations_total",
        description="Total background operations executed",
        unit="1"
    )
    
    bg_duration = meter.create_histogram(
        name="background_operation_duration_seconds",
        description="Background operation duration",
        unit="s"
    )
    
    bg_errors = meter.create_counter(
        name="background_errors_total",
        description="Total background operation errors",
        unit="1"
    )
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        operation_name, child_spans = random.choice(operations)
        start_time = time.time()
        
        # Create a root span for this background operation
        with tracer.start_as_current_span(f"bg-{operation_name}") as root_span:
            root_span.set_attribute("operation.type", "background")
            root_span.set_attribute("operation.cycle", cycle_count)
            
            log_with_context(
                f"Background operation started: {operation_name}",
                operation=operation_name,
                cycle=cycle_count
            )
            
            success = True
            
            # Execute child operations
            for child_name in child_spans:
                with tracer.start_as_current_span(child_name) as child_span:
                    child_span.set_attribute("step.name", child_name)
                    
                    # Simulate work
                    work_duration = random.uniform(0.05, 0.2)
                    time.sleep(work_duration)
                    
                    # Simulate occasional failures (10% chance)
                    if random.random() < 0.1:
                        success = False
                        child_span.set_attribute("error", True)
                        child_span.add_event("error_occurred", {
                            "error.type": "SimulatedError",
                            "error.message": f"Random failure in {child_name}"
                        })
                        log_with_context(
                            f"Step failed: {child_name}",
                            level="error",
                            operation=operation_name,
                            step=child_name,
                            cycle=cycle_count
                        )
                        bg_errors.add(1, {"operation": operation_name, "step": child_name})
                    else:
                        child_span.add_event("step_completed", {"duration_ms": int(work_duration * 1000)})
                        log_with_context(
                            f"Step completed: {child_name}",
                            operation=operation_name,
                            step=child_name,
                            duration_ms=int(work_duration * 1000)
                        )
            
            # Record metrics
            duration = time.time() - start_time
            bg_operations.add(1, {"operation": operation_name, "status": "success" if success else "failure"})
            bg_duration.record(duration, {"operation": operation_name})
            
            root_span.set_attribute("operation.success", success)
            root_span.set_attribute("operation.duration_ms", int(duration * 1000))
            
            log_with_context(
                f"Background operation completed: {operation_name}",
                operation=operation_name,
                success=success,
                duration_ms=int(duration * 1000),
                cycle=cycle_count
            )
        
        # Wait before next operation (3-8 seconds)
        time.sleep(random.uniform(3, 8))


if __name__ == "__main__":
    logger.info("Starting test-app on port 8080")
    
    # Start background MLT emitter thread
    emitter_thread = threading.Thread(target=background_mlt_emitter, daemon=True)
    emitter_thread.start()
    logger.info("Background MLT emitter started - generating traces, logs, and metrics every 3-8 seconds")
    
    app.run(host="0.0.0.0", port=8080, debug=False)
