package jarvey.assoc.motion.tool;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

import jarvey.assoc_feature.TrackFeatureSerde;
import jarvey.streams.node.TrackFeature;
import jarvey.streams.updatelog.KeyedUpdateLogIndexBuilder;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTrackFeatureIndexMain {
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		KeyedUpdateLogIndexBuilder<TrackFeature> cmd = new KeyedUpdateLogIndexBuilder<>();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				Serde<TrackFeature> serde = TrackFeatureSerde.s_serde;
				
				cmd.setApplicationId("track-features-indexer")
					.setInputTopic("track-features")
					.setUpdateDelimitTopic("track-features-delimit")
					.setIndexTableName("track_features_index")
					.useKeyedUpdateSerde(serde)
					.setAutoOffsetReset(AutoOffsetReset.EARLIEST);
				
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
