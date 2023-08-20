package jarvey.assoc.motion;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */

public class MotionBasedAssociatorMain {
	@SuppressWarnings("deprecation")
	public static final void main(String... args) throws Exception {
		MotionBasedAssociatorTopologyBuilder cmd = new MotionBasedAssociatorTopologyBuilder();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				cmd.run();
			}
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
