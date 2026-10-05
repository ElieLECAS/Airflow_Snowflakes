-- ============================================================================
-- 02_cle_publique.sql : enregistre la clé publique de l'utilisateur de service
--
-- À lancer APRÈS 01_entrepot.sql. La clé publique peut figurer dans Git sans risque :
-- seule la clé privée (rsa_key.p8, dans ~/.ssh/snowflake/) doit rester secrète.
--
-- Pour produire la valeur (une seule ligne, sans les lignes BEGIN et END) :
--     grep -v "BEGIN\|END" ~/.ssh/snowflake/rsa_key.pub | tr -d '\n'; echo
--
-- Un utilisateur accepte deux clés publiques en même temps :
--   RSA_PUBLIC_KEY   : clé de la première personne du binôme
--   RSA_PUBLIC_KEY_2 : clé de la seconde (chacun garde sa clé privée, aucune n'est échangée)
-- Pour renouveler une clé sans interruption : poser la nouvelle dans l'emplacement libre,
-- basculer les outils, puis retirer l'ancienne avec ALTER USER ... UNSET RSA_PUBLIC_KEY.
-- ============================================================================
USE ROLE USERADMIN;

ALTER USER AIRFLOW_SVC SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6pbzRcrjplro1l+P1j3AxlMeuSshvDPE9Wmrv6Y4p9reSUjmDeuU9UBi61nXpxCnL0nN6dXm8LZsV96HyBTs0NkO3dfmlwXGSvvinO/kbnuAz9uPDto7hG3kHnCHf5cWek5VKXtFJ/rMw7TJqd5VnPwgHxoTodzDAe6pelMuvSmWGhAlC9FOWF7LAsr+m7934kI212ACdtJ4FjlZS5OtFGinerMQJ61qj12jJKaPTJjn0BA05mXXMmaqWwWJRckyhU1MMVdJGGvLmAjIpFdMUPifYGOQGTnHW9MNi25dvTlveJ79YKDvnBRzgDSzvjF2Ax+tSYKUVMiXPNCsB35anwIDAQAB';

-- Seconde personne du binôme : décommenter et coller sa propre clé publique.
-- ALTER USER AIRFLOW_SVC SET RSA_PUBLIC_KEY_2 = '<clé publique de la seconde personne, sur une ligne>';

-- Vérification : RSA_PUBLIC_KEY_FP doit valoir SHA256:tFiZhc+xrBBmAd3ySSJhZ0DitFgnXkYLNmFka+Ddx4Y=
DESC USER AIRFLOW_SVC;
