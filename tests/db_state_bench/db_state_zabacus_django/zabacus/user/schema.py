import urllib, json
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils.html import escape
import graphene
from graphql import GraphQLError
from graphene_django import DjangoObjectType
from zabacus.bills.schema import get_auth_user


class UserType(DjangoObjectType):
    class Meta:
        model = get_user_model()
        only_fields = ('id', 'username', 'first_name', 'last_name', 'email')


class CreateUser(graphene.Mutation):
    user = graphene.Field(UserType)

    class Arguments:
        username = graphene.String(required=True)
        first_name = graphene.String(required=True)
        last_name = graphene.String(required=True)
        password = graphene.String(required=True)
        email = graphene.String(required=True)
        recaptcha = graphene.String(required=True)

    def mutate(self, info, username, first_name, last_name, password, email, recaptcha):
        if not info.context.user.is_anonymous:
            raise GraphQLError('Please log out first.')
        # verify recaptcha first
        url = 'https://www.google.com/recaptcha/api/siteverify'
        values = {
            'secret': settings.GOOGLE_RECAPTCHA_SECRET_KEY,
            'response': recaptcha
        }
        data = urllib.parse.urlencode(values).encode()
        req = urllib.request.Request(url, data=data)
        response = urllib.request.urlopen(req)
        result = json.loads(response.read().decode())

        if not result['success']:
            raise GraphQLError('reCaptcha verification failed.')

        user = get_user_model()(
            username=escape(username),
            first_name=escape(first_name),
            last_name=escape(last_name),
            email=escape(email)
        )
        user.set_password(password)
        user.save()

        return CreateUser(user=user)


class UpdateUser(graphene.Mutation):
    user = graphene.Field(UserType)

    class Arguments:
        first_name = graphene.String()
        last_name = graphene.String()
        old_password = graphene.String()
        new_password = graphene.String()
        email = graphene.String()

    def mutate(self, info, **kwargs):
        user = get_auth_user(info)
        fn = kwargs.get('first_name')
        ln = kwargs.get('last_name')
        em = kwargs.get('email')
        op = kwargs.get('old_password')
        np = kwargs.get('new_password')
        if fn is not None:
            user.first_name = escape(fn)
        if ln is not None:
            user.last_name = escape(ln)
        if em is not None:
            user.email = escape(em)
        if np is not None:
            if op is None:
                raise GraphQLError('Please enter old password.')
            if user.check_password(op):
                user.set_password(np)
            else:
                raise GraphQLError('Incorrect old password.')
        user.save()
        return UpdateUser(user=user)


class Mutation(graphene.ObjectType):
    create_user = CreateUser.Field()
    update_user = UpdateUser.Field()


class Query(graphene.ObjectType):
    me = graphene.Field(UserType)

    def resolve_me(self, info, **kwargs):
        user = get_auth_user(info)
        return user
