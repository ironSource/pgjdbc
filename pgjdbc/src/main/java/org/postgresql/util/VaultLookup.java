package org.postgresql.util;

import java.util.Map;
import java.util.HashMap;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;


public class VaultLookup {
    private final String user;
    private final String pass;
    private final RenewalConfig renewConfig;
    
    public static final class RenewalConfig {
        public final VaultConfig vltConfig;
        public final String leaseId;
        public final Number initialTTL;
        
        private RenewalConfig (VaultConfig vltConfig, String leaseId, Number initialTTL) {
            this.vltConfig = vltConfig;
            this.leaseId = leaseId;
            this.initialTTL = initialTTL;
        }
    }
    
    public VaultLookup(String VaultAddr, String VaultAuthPath, String VaultDBPath, String VaultUser, String VaultPass) throws VaultException {
        final VaultConfig vltConfig = new VaultConfig(VaultAddr);
        final Vault vault = new Vault(vltConfig);
        Number ttl = 1;
        String token;
    
        token = vault.auth().loginByUserPass(VaultAuthPath, VaultUser, VaultPass).getAuthClientToken();
        vltConfig.token(token);
        try {
            final LogicalResponse res = vault.logical().read(VaultDBPath);
            user = res.getData().get("username");
            pass = res.getData().get("password");
            ttl = res.getLeaseDuration();
            renewConfig = new RenewalConfig(vltConfig, res.getLeaseId(), ttl);
        } finally {
            // Don't logout immediately, as it would revoke the DB credentials before we login.
            // Instead logout after the db creds expire (or in 1 second if we didn't get creds)
            // This won't work in case the DB driver explicitly disconnects the user when the credentials are revoked.
            // In that case, it would be better to create a token role which can create an orphaned token which can create the database credentials
            final Map<String, Object> params = new HashMap();
            params.put("increment", ttl);
            vault.logical().write("auth/token/renew-self", params);
        }
        
    }
    
    public String getUser() {
        return user;
    }
    public String getPass() {
        return pass;
    }
    public RenewalConfig getRenewalConfig() {
        return renewConfig;
    }
}