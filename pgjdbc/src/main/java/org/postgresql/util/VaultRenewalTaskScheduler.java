package org.postgresql.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.util.SharedTimer;
import org.postgresql.util.VaultLookup;
import org.postgresql.jdbc.PgConnection;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.response.LogicalResponse;

// The renewal task latches onto an existing thread scheduler on the connection object
// Closing the connection will automatically purge the scheduler and stop renewing the lease

public class VaultRenewalTaskScheduler {
    private final SharedTimer timer;
    private static final Logger LOGGER = Logger.getLogger(VaultRenewalTask.class.getName());
    
    private class RenewalTask extends TimerTask {
        private final String leaseId;
        private final PgConnection conn;
        private final VaultConfig vltConfig;
        public RenewalTask(PgConnection conn, VaultLookup.RenewalConfig renewConfig) {
            this.conn = conn;
            this.vltConfig = renewConfig.vltConfig;
            this.leaseId = renewConfig.leaseId;
        }
        
        /**
        * Renew the lease and token with Vault and reschedule
        */
        @Override
        public void run() {
            final Vault vault = new Vault(vltConfig);
            try {
                LOGGER.log(Level.FINE, "Attempting to renew credentials for {0}", leaseId);
                // First, renew the database lease according to the default TTL
                final Map<String, Object> params = new HashMap();
                params.put("lease_id", leaseId);
                final LogicalResponse res = vault.logical().write("sys/renew", params); 
                Number ttl = res.getLeaseDuration();
                // Now renew the token to the same TTL as the database
                params.clear();
                params.put("increment", ttl);
                vault.logical().write("auth/token/renew-self", params);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage().toString());
                // Retries are caught and dealt with by the Vault driver, so anything else is fatal.
                // Attempt to close the connection.
                try {
                    conn.close();
                    VaultRenewalTaskScheduler.this.shutdown();
                } catch (Exception e2) {}
            }
        } 
    }
    
    public VaultRenewalTaskScheduler(PgConnection conn, VaultLookup.RenewalConfig renewConfig) {
        final RenewalTask task = new RenewalTask(conn, renewConfig);
        this.timer = new SharedTimer();
        final long nextInterval = this.interval(renewConfig.initialTTL);
        timer.getTimer().schedule(task, nextInterval, nextInterval);
        LOGGER.log(Level.INFO, "Scheduling vault renewal every {0}ms", String.valueOf(nextInterval));
    }
    

    public void shutdown() {
        LOGGER.log(Level.FINER, "Shut down timer");
        timer.releaseTimer();
    }
    
    /**
    * Return the scheduling interval.  Defautls to 2/3 of the lease ttl
    *
    * @param ttl Lease ttl
    */    
    public long interval(Number ttl) {
        return (long)((Long)ttl * 1000 * (2.0f / 3));
    }
}