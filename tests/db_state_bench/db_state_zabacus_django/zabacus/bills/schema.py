import numbers
import graphene
from graphql import GraphQLError
from graphene_django.types import DjangoObjectType
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
import django.utils.timezone as tz
from django.utils.html import escape
from django.contrib.auth import get_user_model
from zabacus.bills.models import Bill, BillItem, Involvement, ItemWeightAssignment, BillStatus


# Helper functions
# Return the currently authorized user
def get_auth_user(info):
    user = info.context.user
    if user.is_anonymous:
        raise GraphQLError('Not logged in.')
    return user


# Check validity of `assignment` JSON string, given a bill and a grand total of the item
def validate_weight_assignment(bill, total, weights):
    validated_weights = []
    assign_total = 0.0
    for username, amount in weights.items():
        if not isinstance(amount, numbers.Real):
            raise GraphQLError('Invalid weight assignment (type error).')
        try:
            assignee = bill.people.get(username=username)
        except ObjectDoesNotExist:
            raise GraphQLError('Invalid weight assignment (user not involved).')
        assign_total += amount
        validated_weights.append((assignee, amount))

    if abs(assign_total - total) >= 0.01:
        raise GraphQLError('Invalid weight assignment (sum mismatch).')
    return validated_weights


class DisplayUserType(DjangoObjectType):
    class Meta:
        model = get_user_model()
        only_fields = ('id', 'username', 'first_name', 'last_name', 'email')


class BillItemType(DjangoObjectType):
    class Meta:
        model = BillItem


class BillType(DjangoObjectType):
    class Meta:
        model = Bill


class ItemWeightAssignmentType(DjangoObjectType):
    class Meta:
        model = ItemWeightAssignment


# Add/Remove items from a bill
class AddBillItem(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        bid = graphene.ID(required=True)
        iname = graphene.String(required=True)
        idesc = graphene.String(required=True)
        payer = graphene.ID(required=True)
        total = graphene.Float(required=True)
        weights = graphene.JSONString(required=True)

    def mutate(self, info, bid, iname, idesc, payer, total, weights):
        user = get_auth_user(info)
        try:
            bill = user.involved.get(id=bid)
        except ObjectDoesNotExist:
            raise GraphQLError('Bill not found.')

        # check validity of the `assign` JSON string
        validated_weights = validate_weight_assignment(bill, total, weights)

        try:
            payer_user = bill.people.get(id=payer)
        except ObjectDoesNotExist:
            raise GraphQLError('Payor not involved.')

        datetime = tz.localtime(tz.now())
        new_item = BillItem.objects.create(
            name=escape(iname),
            desc=escape(idesc),
            date=datetime,
            edited=datetime,
            created_by=user,
            paid_by=payer_user,
            bill=bill,
            total=total
        )
        new_item.save()
        for (u, amount) in validated_weights:
            ass_rel = ItemWeightAssignment.objects.create(
                item=new_item,
                user=u,
                amount=amount
            )
            ass_rel.save()

        return AddBillItem(bill=bill)


class DeleteBillItem(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        iid = graphene.ID(required=True)

    def mutate(self, info, iid):
        user = get_auth_user(info)
        try:
            item = user.created_items.get(id=iid)
        except ObjectDoesNotExist:
            raise GraphQLError('Invalid item.')
        bill = item.bill
        item.delete()

        return DeleteBillItem(bill=bill)


class UpdateBillItem(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        iid = graphene.ID(required=True)
        iname = graphene.String()
        idesc = graphene.String()
        payer = graphene.ID()
        total = graphene.Float()
        weights = graphene.JSONString()

    def mutate(self, info, iid, **kwargs):
        user = get_auth_user(info)
        try:
            item = user.created_items.get(id=iid)
        except ObjectDoesNotExist:
            raise GraphQLError('Item not found.')

        bill = item.bill

        it_nam = kwargs.get('iname')
        it_dec = kwargs.get('idesc')
        it_pyr = kwargs.get('payer')
        it_tot = kwargs.get('total')
        it_wei = kwargs.get('weights')

        if it_nam is not None:
            item.name = escape(it_nam)
        if it_dec is not None:
            item.desc = escape(it_dec)
        if it_pyr is not None:
            try:
                new_payer = bill.people.get(id=it_pyr)
            except ObjectDoesNotExist:
                raise GraphQLError('Payer not involved.')
            item.paid_by = new_payer
        if it_tot is not None:
            if it_wei is None:
                raise GraphQLError('Weight assignment required.')
            validated_weights = validate_weight_assignment(bill, it_tot, it_wei)
            item.total = it_tot
            item.save()
            # remove old weight assignments
            for a in item.assignments.all():
                a.delete()
            for (u, amount) in validated_weights:
                rel = ItemWeightAssignment.objects.create(
                    item=item,
                    user=u,
                    amount=amount
                )
                rel.save()

        return UpdateBillItem(bill=bill)


class BenchmarkUpdateBillName(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        bname = graphene.String()

    def mutate(self, info, bname):
        user = get_auth_user(info)
        try:
            bill = user.involved.first()
        except ObjectDoesNotExist:
            raise GraphQLError('Bill not found.')
        bill.name = escape(bname)
        bill.save()
        return BenchmarkUpdateBillName(bill=bill)


class BenchmarkUpdateBillItemName(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        bid = graphene.ID(required=True)
        iname = graphene.String(required=True)

    def mutate(self, info, iname):
        user = get_auth_user(info)
        try:
            bill = user.involved.first()
            item = bill.items.first()
        except ObjectDoesNotExist:
            raise GraphQLError('Item not found.')

        item.name = escape(iname)
        item.save()
        return BenchmarkUpdateBillItemName(bill=bill)


# Add/remove a user from a bill
class AddUserToBill(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        bid = graphene.ID(required=True)
        uname = graphene.String(required=True)

    def mutate(self, info, bid, uname):
        user = get_auth_user(info)
        try:
            bill = user.involved.get(id=bid)
        except ObjectDoesNotExist:
            raise GraphQLError('Can not find bill.')
        try:
            target_user = get_user_model().objects.get(username=uname)
        except ObjectDoesNotExist:
            raise GraphQLError('Target user does not exist.')
        if bill.people.filter(username=uname).exists():
            raise GraphQLError('User already in bill.')

        new_rel = Involvement.objects.create(bill=bill, user=target_user)
        new_rel.save()

        return AddUserToBill(bill=bill)


class RemoveUserFromBill(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        bid = graphene.ID(required=True)
        uid = graphene.ID(required=True)

    def mutate(self, info, bid, uid):
        user = get_auth_user(info)
        try:
            bill = user.involved.get(id=bid)
        except ObjectDoesNotExist:
            raise GraphQLError('Bill not found.')
        try:
            victim = bill.people.get(id=uid)
        except ObjectDoesNotExist:
            raise GraphQLError('User does not exist.')
        for i in bill.items.all():
            if i.assignments.filter(user=victim).exists():
                raise GraphQLError('User cannot be removed.')
        try:
            rel = Involvement.objects.get(bill=bill, user=victim)
        except ObjectDoesNotExist:
            raise GraphQLError('Impossible.')
        rel.delete()

        return RemoveUserFromBill(bill=bill)


# Create/Remove entire bills
class CreateBill(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        name = graphene.String(required=True)
        desc = graphene.String()
        bid = graphene.ID()

    def mutate(self, info, name, **kwargs):
        user = get_auth_user(info)
        datetime = tz.localtime(tz.now())
        new_bill = Bill.objects.create(
            name=escape(name),
            date=datetime,
            edited=datetime,
            created_by=user,
            status=BillStatus.OPN.name
        )
        bd = kwargs.get('desc')
        if bd is not None:
            new_bill.desc = escape(bd)

        new_bill.save()
        bid = kwargs.get('bid')
        if bid is not None:
            # involve all users present in bill `bid`, if provided
            try:
                bill = user.involved.get(id=bid)
            except ObjectDoesNotExist:
                raise GraphQLError('Template source bill not found.')
            for p in bill.people.all():
                new_rel = Involvement.objects.create(bill=new_bill, user=p)
                new_rel.save()
        else:
            # otherwise only involve the creating user
            new_rel = Involvement.objects.create(bill=new_bill, user=user)
            new_rel.save()

        return CreateBill(bill=new_bill)


class DeleteBill(graphene.Mutation):
    result = graphene.String()

    class Arguments:
        bid = graphene.ID(required=True)

    def mutate(self, info, bid):
        user = get_auth_user(info)
        try:
            bill = user.created.get(id=bid)
        except ObjectDoesNotExist:
            raise GraphQLError('Bill not found.')
        bill.delete()

        return DeleteBill(result='OK')


class UpdateBill(graphene.Mutation):
    bill = graphene.Field(BillType)

    class Arguments:
        bid = graphene.ID(required=True)
        name = graphene.String()
        desc = graphene.String()
        status = graphene.String()

    def mutate(self, info, bid, **kwargs):
        user = get_auth_user(info)
        try:
            bill = user.involved.get(id=bid)
        except ObjectDoesNotExist:
            raise GraphQLError('Bill not found.')
        bn = kwargs.get('name')
        bd = kwargs.get('desc')
        bs = kwargs.get('status')
        if bn is not None:
            bill.name = escape(bn)
        if bd is not None:
            bill.desc = escape(bd)
        if bs is not None:
            bill.status = bs
        bill.save()
        return UpdateBill(bill=bill)


class Mutation(graphene.ObjectType):
    add_user_to_bill = AddUserToBill.Field()
    remove_user_from_bill = RemoveUserFromBill.Field()
    add_bill_item = AddBillItem.Field()
    update_bill_item = UpdateBillItem.Field()
    delete_bill_item = DeleteBillItem.Field()
    create_bill = CreateBill.Field()
    update_bill = UpdateBill.Field()
    delete_bill = DeleteBill.Field()


class Query(graphene.ObjectType):
    list_bills = graphene.List(BillType)
    created_bills = graphene.List(BillType)
    show_bill = graphene.Field(BillType, bid=graphene.ID())

    def resolve_list_bills(self, info):
        user = get_auth_user(info)
        return user.involved.all().order_by('-date')

    def resolve_created_bills(self, info):
        user = get_auth_user(info)
        return user.created.all()

    def resolve_show_bill(self, info, bid):
        user = get_auth_user(info)
        return user.involved.get(id=bid)
