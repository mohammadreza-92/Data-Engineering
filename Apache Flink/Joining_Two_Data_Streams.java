// Order data model
public class Order {
    public String orderId;
    public String customerId;
    public double amount;
    public long timestamp;
    
    public Order() {}
    
    public Order(String orderId, String customerId, double amount, long timestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.timestamp = timestamp;
    }
}

// Customer data model
public class Customer {
    public String customerId;
    public String name;
    public String email;
    public long timestamp;
    
    public Customer() {}
    
    public Customer(String customerId, String name, String email, long timestamp) {
        this.customerId = customerId;
        this.name = name;
        this.email = email;
        this.timestamp = timestamp;
    }
}

// Stream joining implementation
public class StreamJoiner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Orders stream
        DataStream<Order> orders = env
            .addSource(new FlinkKafkaConsumer<>("orders", new JSONKeyValueDeserializationSchema(), properties))
            .map(value -> {
                JSONObject json = (JSONObject) value.get("value");
                return new Order(
                    json.getString("orderId"),
                    json.getString("customerId"),
                    json.getDouble("amount"),
                    json.getLong("timestamp")
                );
            });
        
        // Customers stream
        DataStream<Customer> customers = env
            .addSource(new FlinkKafkaConsumer<>("customers", new JSONKeyValueDeserializationSchema(), properties))
            .map(value -> {
                JSONObject json = (JSONObject) value.get("value");
                return new Customer(
                    json.getString("customerId"),
                    json.getString("name"),
                    json.getString("email"),
                    json.getLong("timestamp")
                );
            });
        
        // Join streams with CoProcessFunction
        DataStream<String> enrichedOrders = orders
            .connect(customers)
            .keyBy(order -> order.customerId, customer -> customer.customerId)
            .process(new OrderEnricher());
        
        enrichedOrders.print();
        
        env.execute("Order Enrichment with Customer Data");
    }
    
    public static class OrderEnricher 
        extends CoProcessFunction<Order, Customer, String> {
        
        private ValueState<Customer> customerState;
        
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Customer> descriptor = 
                new ValueStateDescriptor<>("customerState", Customer.class);
            customerState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void processElement1(
            Order order,
            Context ctx,
            Collector<String> out) throws Exception {
            
            Customer customer = customerState.value();
            if (customer != null) {
                // If customer data exists, enrich the order
                out.collect("Order ID: " + order.orderId + 
                          ", Amount: " + order.amount + 
                          ", Customer: " + customer.name + 
                          ", Email: " + customer.email);
            } else {
                // If customer data not available yet
                out.collect("Order ID: " + order.orderId + 
                          " received but customer data not available yet");
            }
        }
        
        @Override
        public void processElement2(
            Customer customer,
            Context ctx,
            Collector<String> out) throws Exception {
            
            // Store customer information in state
            customerState.update(customer);
            
            // Can send message about new customers
            out.collect("New customer registered: " + customer.name);
        }
    }
}