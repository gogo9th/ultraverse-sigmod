import graphene
import graphql_jwt
import zabacus.bills.schema
import zabacus.user.schema


class Query(zabacus.user.schema.Query, zabacus.bills.schema.Query, graphene.ObjectType):
    pass


class Mutation(zabacus.user.schema.Mutation, zabacus.bills.schema.Mutation, graphene.ObjectType):
    token_auth = graphql_jwt.ObtainJSONWebToken.Field()
    verify_token = graphql_jwt.Verify.Field()
    refresh_token = graphql_jwt.Refresh.Field()


schema = graphene.Schema(query=Query, mutation=Mutation)
