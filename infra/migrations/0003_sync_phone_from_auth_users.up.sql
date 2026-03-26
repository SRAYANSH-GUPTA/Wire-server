CREATE OR REPLACE FUNCTION public.sync_phone_from_auth_users()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_phone TEXT;
BEGIN
    v_phone := NULLIF(TRIM(NEW.raw_user_meta_data ->> 'phone'), '');

    INSERT INTO public.users (id, email, phone, updated_at)
    VALUES (NEW.id::text, NEW.email, v_phone, NOW())
    ON CONFLICT (id)
    DO UPDATE SET
        phone = EXCLUDED.phone,
        email = COALESCE(EXCLUDED.email, public.users.email),
        updated_at = NOW();

    RETURN NEW;
EXCEPTION
    WHEN unique_violation THEN
        -- Do not block auth writes if phone conflicts with existing user record.
        RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_phone_from_auth_users ON auth.users;

CREATE TRIGGER trg_sync_phone_from_auth_users
AFTER INSERT OR UPDATE OF raw_user_meta_data, email
ON auth.users
FOR EACH ROW
EXECUTE FUNCTION public.sync_phone_from_auth_users();

