package org.apache.flink.examples.datapreparation;

import com.github.rnowling.bps.datagenerator.datamodels.Customer;
import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.InputData;
import com.github.rnowling.bps.datagenerator.framework.SeedFactory;
import com.github.rnowling.bps.datagenerator.framework.samplers.Sampler;
import com.github.rnowling.bps.datagenerator.generators.customer.CustomerSamplerBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * Generate Random Customers
 */
public class CustomerGenerator implements SourceFunction<Customer> {

  final Sampler<Customer> customerSampler;
  private final List<Store> storeList;
  private volatile boolean isRunning = true;

  public CustomerGenerator(InputData inputData, List<Store> stores, SeedFactory seedFactory) {
    storeList = stores;
    CustomerSamplerBuilder builder = new CustomerSamplerBuilder(stores, inputData, seedFactory);
    customerSampler = builder.build();
  }

  @Override
  public void run(SourceContext<Customer> sourceContext) throws Exception {
    for (int i = 0; i < storeList.size(); i++) {
      sourceContext.collect(customerSampler.sample());
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
