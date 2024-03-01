using MySqlConnector;
using Utils.S23S.NuGet.CipherAES256;
using Utils.S23S.NuGet.Response;

namespace MYSQLConnector.S23S.NuGet
{
    public class S23SMYSQLConnector
    {
        private string connectionString;
        private string query;
        private Dictionary<string, object>? parameters;
        private string? pwdAES;
        public S23SMYSQLConnector(string connectionString, string query, string? pwdAES = null)
        {
            this.connectionString = connectionString;
            this.query = query;
            this.pwdAES = pwdAES;
        }
        public S23SMYSQLConnector(string connectionString, string query, Dictionary<string, object>? parameters, string? pwdAES = null)
        {
            this.connectionString = connectionString;
            this.query = query;
            this.parameters = parameters;
            this.pwdAES = pwdAES;
        }
        public ApiResult GetQuery()
        {
            try
            {
                if (String.IsNullOrWhiteSpace(connectionString))
                {
                    return new ApiResult(StateOperation.InvalidRequest, true, $"La cadena de conexión está vacía. Seg: {nameof(GetQuery)}");
                }

                if (!String.IsNullOrWhiteSpace(pwdAES))
                {
                    ApiResult ciph = CipherAES.Cipher(connectionString, pwdAES.PadRight(32, 'S'), false);
                    if (ciph.code != StateOperation.OK)
                    {
                        return new ApiResult(StateOperation.InvalidRequest, true, $"{ciph.message} - Seg: {nameof(GetQuery)}");
                    }

                    connectionString = ciph.oDyn?.ToString() ?? String.Empty;
                }


                List<object[]> list = new List<object[]>();
                StateOperation state;

                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();
                    MySqlCommand cmd = new MySqlCommand();

                    cmd.Connection = connection;
                    cmd.CommandText = query;

                    if(parameters != null)
                    {
                        foreach (KeyValuePair<string, object> entry in parameters)
                        {
                            cmd.Parameters.Add(entry.Key, ParseDbType(entry.Value)).Value = entry.Value;
                        }
                    }

                    using(MySqlDataReader reader = cmd.ExecuteReader())
                    {
                        state = (reader.HasRows) ? StateOperation.OK : StateOperation.NoContent;
                        while (reader.Read())
                        {
                            object[] values = new object[reader.FieldCount];
                            reader.GetValues(values);

                            list.Add(values);
                        }
                    }

                    connection.Close();
                }

                ApiResult apiResult = new ApiResult(state, list);

                return apiResult;
            }
            catch (Exception ex)
            {
                return new ApiResult(StateOperation.InternalServerError, ex);
            }
        }

        public ApiResult InsertQuery()
        {
            try
            {
                if (String.IsNullOrWhiteSpace(connectionString))
                {
                    return new ApiResult(StateOperation.InvalidRequest, true, $"La cadena de conexión está vacía. Seg: {nameof(InsertQuery)}");
                }
                if (parameters == null)
                {
                    return new ApiResult(StateOperation.InvalidRequest, true, "Sin parámetros para insertar el registro");
                }

                if (!String.IsNullOrWhiteSpace(pwdAES))
                {
                    ApiResult ciph = CipherAES.Cipher(connectionString, pwdAES.PadRight(32, 'S'), false);
                    if (ciph.code != StateOperation.OK)
                    {
                        return new ApiResult(StateOperation.InvalidRequest, true, $"{ciph.message} - Seg: {nameof(GetQuery)}");
                    }

                    connectionString = ciph.oDyn?.ToString() ?? String.Empty;
                }


                List<object[]> list = new List<object[]>();
                ApiResult result;

                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();
                    MySqlCommand cmd = new MySqlCommand();

                    cmd.Connection = connection;
                    cmd.CommandText = query;

                    foreach (KeyValuePair<string, object> entry in parameters)
                    {
                        cmd.Parameters.Add(entry.Key, ParseDbType(entry.Value)).Value = entry.Value;
                    }

                    cmd.ExecuteNonQuery();
                    long insertedId = cmd.LastInsertedId;

                    if (insertedId > 0)
                    {
                        result = new ApiResult(StateOperation.OK, insertedId);
                    }
                    else
                    {
                        result = new ApiResult(StateOperation.InternalServerError, new Exception("Error no controlado al insertar el registro"));
                    }

                    connection.Close();

                    return result;
                }
            }
            catch (Exception ex)
            {
                return new ApiResult(StateOperation.InternalServerError, ex);
            }
        }

        public ApiResult UpdateQuery()
        {
            try
            {
                ApiResult result;
                int cantRowsAffected = 0;

                if (String.IsNullOrWhiteSpace(connectionString))
                {
                    return new ApiResult(StateOperation.InvalidRequest, true, $"La cadena de conexión está vacía. Seg: {nameof(UpdateQuery)}");
                }
                if (parameters == null)
                {
                    return new ApiResult(StateOperation.InvalidRequest, true, "Sin parámetros para actualizar el registro");
                }

                if (!String.IsNullOrWhiteSpace(pwdAES))
                {
                    ApiResult ciph = CipherAES.Cipher(connectionString, pwdAES.PadRight(32, 'S'), false);
                    if (ciph.code != StateOperation.OK)
                    {
                        return new ApiResult(StateOperation.InvalidRequest, true, $"{ciph.message} - Seg: {nameof(GetQuery)}");
                    }

                    connectionString = ciph.oDyn?.ToString() ?? String.Empty;
                }


                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();
                    MySqlCommand cmd = new MySqlCommand();

                    cmd.Connection = connection;
                    cmd.CommandText = query;

                    foreach (KeyValuePair<string, object> entry in parameters)
                    {
                        cmd.Parameters.Add(entry.Key, ParseDbType(entry.Value)).Value = entry.Value;
                    }

                    using (MySqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            cantRowsAffected = Convert.ToInt32(reader.GetValue(0));
                        }
                    }


                    if (cantRowsAffected > 0)
                    {
                        result = new ApiResult(StateOperation.OK, cantRowsAffected);
                    }
                    else
                    {
                        result = new ApiResult(StateOperation.InternalServerError, new Exception("Error no controlado al actualizar el registro"));
                    }

                    connection.Close();

                    return result;
                }
            }
            catch (Exception ex)
            {
                return new ApiResult(StateOperation.InternalServerError, ex);
            }
        }

        private MySqlDbType ParseDbType(object val)
        {
            if (val.GetType() == typeof(int))
            {
                return MySqlDbType.Int32;
            }
            else if (val.GetType() == typeof(string))
            {
                return MySqlDbType.VarChar;
            }
            else if (val.GetType() == typeof(DateTime))
            {
                return MySqlDbType.DateTime;
            }
            else if(val is DBNull)
            {
                return MySqlDbType.Null;
            }
            else
            {
                return MySqlDbType.VarChar;
            }
        }
    }
}
