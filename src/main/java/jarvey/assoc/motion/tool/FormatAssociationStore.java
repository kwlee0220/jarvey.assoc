package jarvey.assoc.motion.tool;

import java.sql.Connection;

import utils.UsageHelp;
import utils.func.CheckedRunnable;
import utils.jdbc.JdbcParameters;
import utils.jdbc.JdbcProcessor;

import jarvey.assoc.AssociationStore;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FormatAssociationStore implements CheckedRunnable {
	@Mixin private UsageHelp m_help;
	@Mixin private JdbcParameters m_jdbcParams;
	
	public void run() throws Exception {
		JdbcProcessor jdbc = m_jdbcParams.createJdbcProcessor();
		try ( Connection conn = jdbc.connect() ) {
			AssociationStore.dropTable(conn);
			AssociationStore.createTable(conn);
		}
	}
	
	public static final void main(String... args) throws Exception {
		FormatAssociationStore cmd = new FormatAssociationStore();
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
