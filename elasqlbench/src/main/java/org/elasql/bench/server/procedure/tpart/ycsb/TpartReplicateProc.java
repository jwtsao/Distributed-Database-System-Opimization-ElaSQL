package org.elasql.bench.server.procedure.tpart.ycsb;

import java.util.Map;

import org.elasql.bench.server.param.ycsb.ElasqlYcsbReplicateProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class TpartReplicateProc extends TPartStoredProcedure<ElasqlYcsbReplicateProcParamHelper>{

	public TpartReplicateProc(long txNum) {
		super(txNum, new ElasqlYcsbReplicateProcParamHelper());
	}
	
	@Override
	protected void prepareKeys() {
		// set read key
		
		PrimaryKey key = paramHelper.getReadKey();
		addReadKey(key);
		
	}
	
	@Override
	public double getWeight() {
		return 1;
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		//do anything?
	}
	
}
