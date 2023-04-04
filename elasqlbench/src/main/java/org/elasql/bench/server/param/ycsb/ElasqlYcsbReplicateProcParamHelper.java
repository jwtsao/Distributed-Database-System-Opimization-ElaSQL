package org.elasql.bench.server.param.ycsb;

import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ElasqlYcsbReplicateProcParamHelper extends StoredProcedureParamHelper{

	private PrimaryKey replicateKey;
	
	public PrimaryKey getReadKey() {
		return replicateKey;
	}
	
	public void prepareParameters(Object... pars) {
		replicateKey = (PrimaryKey) pars[0];
	}
	
	@Override
	public Schema getResultSetSchema() {
		Schema sch = new Schema();
		return sch;
	}
	
	@Override
	public SpResultRecord newResultSetRecord() {
		SpResultRecord rec = new SpResultRecord();
		return rec;
	}
}

