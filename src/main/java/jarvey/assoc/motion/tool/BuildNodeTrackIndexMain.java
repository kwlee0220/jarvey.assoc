package jarvey.assoc.motion.tool;

import org.apache.kafka.streams.Topology.AutoOffsetReset;

import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;
import jarvey.streams.updatelog.KeyedUpdateLogIndexBuilder;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildNodeTrackIndexMain {
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		KeyedUpdateLogIndexBuilder<NodeTrack> cmd = new KeyedUpdateLogIndexBuilder<>();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.setApplicationId("node-track-indexer")
					.setInputTopic("node-tracks-repartition")
					.useKeyedUpdateSerde(GsonUtils.getSerde(NodeTrack.class))
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
