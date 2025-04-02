// Sensor data model
public class SensorReading {
    public String sensorId;
    public double value;
    public long timestamp;
    
    public SensorReading() {}
    
    public SensorReading(String sensorId, double value, long timestamp) {
        this.sensorId = sensorId;
        this.value = value;
        this.timestamp = timestamp;
    }
}

// Rolling average calculation with KeyedProcessFunction
public class RollingAverageExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Create sample sensor data stream
        DataStream<SensorReading> sensorData = env
            .addSource(new FlinkKafkaConsumer<>("sensor-topic", new SimpleStringSchema(), properties))
            .map(value -> {
                String[] parts = value.split(",");
                return new SensorReading(parts[0], Double.parseDouble(parts[1]), Long.parseLong(parts[2]));
            });
        
        // Calculate rolling average for each sensor
        DataStream<String> averages = sensorData
            .keyBy(r -> r.sensorId)
            .process(new RollingAverageCalculator(5)); // Average of last 5 readings
            
        averages.print();
        
        env.execute("Rolling Average Example");
    }
    
    public static class RollingAverageCalculator 
        extends KeyedProcessFunction<String, SensorReading, String> {
        
        private final int windowSize;
        private ListState<Double> valuesState;
        
        public RollingAverageCalculator(int windowSize) {
            this.windowSize = windowSize;
        }
        
        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<Double> descriptor = 
                new ListStateDescriptor<>("sensorValues", Double.class);
            valuesState = getRuntimeContext().getListState(descriptor);
        }
        
        @Override
        public void processElement(
            SensorReading reading,
            Context ctx,
            Collector<String> out) throws Exception {
            
            // Add new value to state
            valuesState.add(reading.value);
            
            // Get current values
            Iterable<Double> currentValues = valuesState.get();
            List<Double> valuesList = new ArrayList<>();
            currentValues.forEach(valuesList::add);
            
            // Remove oldest if exceeding window size
            if (valuesList.size() > windowSize) {
                valuesList.remove(0);
                valuesState.update(valuesList);
            }
            
            // Calculate average
            double sum = 0.0;
            for (Double val : valuesList) {
                sum += val;
            }
            double average = sum / valuesList.size();
            
            out.collect("Sensor " + reading.sensorId + 
                       " - Average: " + average + 
                       " (based on " + valuesList.size() + " values)");
        }
    }
}