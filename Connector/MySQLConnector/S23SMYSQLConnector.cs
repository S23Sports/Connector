using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utils.S23S.NuGet.Response;

namespace MYSQLConnector.S23S.NuGet
{
    public class S23SMYSQLConnector
    {
        private string connectionString;
        private string query;
        private Dictionary<string, object>? parameters;
        public S23SMYSQLConnector(string connectionString, string query)
        {
            this.connectionString = connectionString;
            this.query = query;
        }
        public S23SMYSQLConnector(string connectionString, string query, Dictionary<string, object>? parameters)
        {
            this.connectionString = connectionString;
            this.query = query;
            this.parameters = parameters;
        }
        public ApiResult GetQuery()
        {
            try
            {
                if (String.IsNullOrWhiteSpace(connectionString))
                {
                    return new ApiResult(StatusCodes.InvalidRequest, true, $"La cadena de conexión está vacía. Seg: {nameof(GetQuery)}");
                }

                List<object[]> list = new List<object[]>();
                StatusCodes state;

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
                        state = (reader.HasRows) ? StatusCodes.OK : StatusCodes.NoContent;
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
                return new ApiResult(StatusCodes.InternalServerError, ex);
            }
        }

        public ApiResult InsertQuery()
        {
            try
            {
                if (String.IsNullOrWhiteSpace(connectionString))
                {
                    return new ApiResult(StatusCodes.InvalidRequest, true, $"La cadena de conexión está vacía. Seg: {nameof(InsertQuery)}");
                }
                if (parameters == null)
                {
                    return new ApiResult(StatusCodes.InvalidRequest, true, "Sin parámetros para insertar el registro");
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
                        result = new ApiResult(StatusCodes.OK, insertedId);
                    }
                    else
                    {
                        result = new ApiResult(StatusCodes.InternalServerError, new Exception("Error no controlado al insertar el registro"));
                    }

                    connection.Close();

                    return result;
                }
            }
            catch (Exception ex)
            {
                return new ApiResult(StatusCodes.InternalServerError, ex);
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
                    return new ApiResult(StatusCodes.InvalidRequest, true, $"La cadena de conexión está vacía. Seg: {nameof(UpdateQuery)}");
                }
                if (parameters == null)
                {
                    return new ApiResult(StatusCodes.InvalidRequest, true, "Sin parámetros para actualizar el registro");
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
                        result = new ApiResult(StatusCodes.OK, cantRowsAffected);
                    }
                    else
                    {
                        result = new ApiResult(StatusCodes.InternalServerError, new Exception("Error no controlado al actualizar el registro"));
                    }

                    connection.Close();

                    return result;
                }
            }
            catch (Exception ex)
            {
                return new ApiResult(StatusCodes.InternalServerError, ex);
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
