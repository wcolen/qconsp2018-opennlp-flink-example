package org.apache.flink.examples.datapreparation;

import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.InputData;
import com.github.rnowling.bps.datagenerator.framework.SeedFactory;
import com.github.rnowling.bps.datagenerator.framework.samplers.Sampler;
import com.github.rnowling.bps.datagenerator.generators.store.StoreSamplerBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

public class StoreGenerator implements SourceFunction<Store> {

  final Sampler<Store> storeSampler;
  private volatile boolean isRunning = true;
  private int numStores;

  public StoreGenerator(InputData inputData, int numStores, SeedFactory seedFactory) {
    this.numStores = numStores;
    StoreSamplerBuilder builder = new StoreSamplerBuilder(inputData.getZipcodeTable(), seedFactory);
    storeSampler = builder.build();
  }

  @Override
  public void run(SourceContext<Store> sourceContext) throws Exception {
    for (int i = 0; i < numStores; i++) {
      sourceContext.collect(storeSampler.sample());
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
