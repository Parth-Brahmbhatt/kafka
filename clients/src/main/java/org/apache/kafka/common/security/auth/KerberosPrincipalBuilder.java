package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Map;

public class KerberosPrincipalBuilder implements PrincipalBuilder {

    private KerberosShortNamer kerberosShortNamer;

    @Override
    public void configure(Map<String, ?> configs) {
        String defaultRealm;
        try {
            defaultRealm = JaasUtils.defaultRealm();
        } catch (Exception ke) {
            defaultRealm = "";
        }

        List<String> principalToLocalRules = (List<String>) configs.get(SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES);
        if (principalToLocalRules != null) {
            kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
        }

    }

    @Override
    public Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException {
        if(authenticator instanceof SaslServerAuthenticator) {
            String authenticationId = ((SaslServerAuthenticator) authenticator).getSaslServer().getAuthorizationID();
            try {
                kerberosShortNamer.shortName(KerberosName.parse(authenticationId));
            } catch (IOException e) {
                throw new KafkaException("Failed to get short name for: " + authenticationId, e);
            }
        }
        return null;
    }

    @Override
    public void close() throws KafkaException {

    }
}
