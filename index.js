require("dotenv").config();
const {
  Client,
  GatewayIntentBits,
  SlashCommandBuilder,
  Routes,
  REST,
  PermissionsBitField,
  ChannelType,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  StringSelectMenuOptionBuilder,
} = require("discord.js");
const express = require("express");
const bodyParser = require("body-parser");
const Stripe = require("stripe");
const { MongoClient, ObjectId } = require("mongodb");

// ===================== HARD LOCK =====================
const ALLOWED_GUILD_ID = "1269702895549419544";
const REFUND_APPROVER_USER_ID = "1400281740978815118";
const TICKET_PANEL_CHANNEL_ID = "1452386415814901924";

// ===================== BRANDING =====================
const BRAND_NAME = "Vivid Tweaks";
const BRAND_COLOR = 0x8b5cf6;
const BRAND_FOOTER = "Vivid Tweaks";

// ===================== MONEY =====================
const CURRENCY = "gbp";
const REFUND_WINDOW_MS = 24 * 60 * 60 * 1000; // 24 hours

function amountToMinorUnits(amountStr) {
  if (typeof amountStr !== "string") return null;
  const trimmed = amountStr.trim();
  if (!/^\d+(\.\d{1,2})?$/.test(trimmed)) return null;
  const [whole, fracRaw] = trimmed.split(".");
  const frac = (fracRaw || "").padEnd(2, "0").slice(0, 2);
  const minor = Number(whole) * 100 + Number(frac);
  if (!Number.isFinite(minor) || minor <= 0) return null;
  return minor;
}

function minorToDisplay(minor) {
  return `£${(minor / 100).toFixed(2)}`;
}

function makePurchaseId() {
  return `VT-${Date.now().toString(36).toUpperCase()}-${Math.random()
    .toString(36)
    .slice(2, 7)
    .toUpperCase()}`;
}

function makeRefundRequestId() {
  return `RR-${Date.now().toString(36).toUpperCase()}-${Math.random()
    .toString(36)
    .slice(2, 6)
    .toUpperCase()}`;
}

// ===================== PLANS =====================
const PLAN_KEYS = ["one_time", "monthly", "annual", "lifetime"];
const PLAN_LABELS = {
  one_time: "One-time",
  monthly: "Monthly",
  annual: "Annually",
  lifetime: "Lifetime",
};
const PLAN_INTERVAL = {
  monthly: { interval: "month", interval_count: 1 },
  annual: { interval: "year", interval_count: 1 },
};

// ===================== ENV =====================
const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.DISCORD_CLIENT_ID;
const MONGODB_URI = process.env.MONGODB_URI;
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL;
const PORT = Number(process.env.PORT || 3000);

if (!DISCORD_TOKEN) throw new Error("Missing DISCORD_TOKEN");
if (!CLIENT_ID) throw new Error("Missing DISCORD_CLIENT_ID");
if (!MONGODB_URI) throw new Error("Missing MONGODB_URI");
if (!STRIPE_SECRET_KEY) throw new Error("Missing STRIPE_SECRET_KEY");
if (!STRIPE_WEBHOOK_SECRET) throw new Error("Missing STRIPE_WEBHOOK_SECRET");
if (!PUBLIC_BASE_URL) throw new Error("Missing PUBLIC_BASE_URL");

// ===================== STRIPE =====================
const stripe = new Stripe(STRIPE_SECRET_KEY, { apiVersion: "2024-06-20" });

// ===================== DB =====================
const mongo = new MongoClient(MONGODB_URI);
let db;
let productsCol, configCol, purchasesCol, refundRequestsCol, ticketsCol;

async function initDb() {
  await mongo.connect();
  db = mongo.db();
  productsCol = db.collection("products");
  configCol = db.collection("shop_config");
  purchasesCol = db.collection("purchases");
  refundRequestsCol = db.collection("refund_requests");
  ticketsCol = db.collection("tickets");

  await productsCol.createIndex({ guildId: 1, createdAt: -1 });
  await configCol.createIndex({ guildId: 1 }, { unique: true });
  await purchasesCol.createIndex({ purchaseId: 1 }, { unique: true });
  await purchasesCol.createIndex({ stripeSessionId: 1 }, { unique: true, sparse: true });
  await purchasesCol.createIndex({ stripeSubscriptionId: 1 }, { sparse: true });
  await purchasesCol.createIndex({ guildId: 1, paidAt: -1 });
  await refundRequestsCol.createIndex({ requestId: 1 }, { unique: true });
  await refundRequestsCol.createIndex({ guildId: 1, createdAt: -1 });
  await ticketsCol.createIndex({ guildId: 1, userId: 1, status: 1 });
  await ticketsCol.createIndex({ guildId: 1, channelId: 1 }, { unique: true });

  console.log("✅ MongoDB connected");
}

// ===================== DISCORD =====================
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

function ensureAllowedGuild(interaction) {
  if (!interaction.guild || interaction.guild.id !== ALLOWED_GUILD_ID) {
    interaction.reply({
      content: "This bot is locked to one server. Not this one.",
      ephemeral: true,
    }).catch(() => {});
    return false;
  }
  return true;
}

function isAdmin(interaction) {
  const member = interaction.member;
  return (
    member?.permissions &&
    new PermissionsBitField(member.permissions).has(PermissionsBitField.Flags.Administrator)
  );
}

function requireAdmin(interaction) {
  if (!isAdmin(interaction)) {
    interaction.reply({ content: "Administrator permission required.", ephemeral: true }).catch(() => {});
    return false;
  }
  return true;
}

// ===================== CONFIG =====================
async function getConfig(guildId) {
  return (await configCol.findOne({ guildId })) || null;
}

async function upsertConfig(guildId, patch) {
  await configCol.updateOne(
    { guildId },
    { $set: { ...patch, updatedAt: new Date() } },
    { upsert: true }
  );
}

// ===================== PRODUCT HELPERS =====================
function enabledPlans(pricesObj) {
  const out = [];
  for (const key of PLAN_KEYS) {
    if (pricesObj?.[key]?.amountMinor) out.push(key);
  }
  return out;
}

function formatPlans(pricesObj) {
  const plans = enabledPlans(pricesObj);
  if (!plans.length) return "None";
  return plans
    .map((k) => `\`${PLAN_LABELS[k]} ${minorToDisplay(pricesObj[k].amountMinor)}\``)
    .join(", ");
}

function planButtonsForTicket(product) {
  const plans = enabledPlans(product.prices);
  if (!plans.length) return [];
  const row = new ActionRowBuilder();
  for (const planKey of plans) {
    row.addComponents(
      new ButtonBuilder()
        .setCustomId(`tp:${product._id.toString()}:${planKey}`)
        .setStyle(ButtonStyle.Primary)
        .setLabel(`${PLAN_LABELS[planKey]} ${minorToDisplay(product.prices[planKey].amountMinor)}`)
    );
  }
  row.addComponents(
    new ButtonBuilder()
      .setCustomId(`close_ticket`)
      .setStyle(ButtonStyle.Danger)
      .setLabel("Close Ticket")
  );
  return [row];
}

// ===================== CHANNEL HELPERS =====================
async function ensurePurchaseLogChannel(guild) {
  const cfg = await getConfig(guild.id);
  if (cfg?.purchaseLogChannelId) {
    const ch = await guild.channels.fetch(cfg.purchaseLogChannelId).catch(() => null);
    if (ch && ch.type === ChannelType.GuildText) return ch;
  }
  const existing = guild.channels.cache.find(
    (c) => c.type === ChannelType.GuildText && c.name === "purchase-log"
  );
  if (existing) {
    await upsertConfig(guild.id, { purchaseLogChannelId: existing.id });
    return existing;
  }
  const created = await guild.channels.create({
    name: "purchase-log",
    type: ChannelType.GuildText,
    permissionOverwrites: [
      { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
    ],
  });
  await upsertConfig(guild.id, { purchaseLogChannelId: created.id });
  return created;
}

async function logToPurchaseLog(guild, embed, components = []) {
  const ch = await ensurePurchaseLogChannel(guild);
  await ch.send({ embeds: [embed], components }).catch(() => {});
}

async function sendThanksIfConfigured(guild, userId, purchaseId) {
  const cfg = await getConfig(guild.id);
  if (!cfg?.thanksChannelId) return;
  const ch = await guild.channels.fetch(cfg.thanksChannelId).catch(() => null);
  if (!ch || ch.type !== ChannelType.GuildText) return;
  const embed = new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Thank you!`)
    .setDescription(`Thanks for buying <@${userId}> 💜\nPurchase ID: \`${purchaseId}\``)
    .setFooter({ text: BRAND_FOOTER })
    .setTimestamp(new Date());
  await ch.send({ embeds: [embed] }).catch(() => {});
}

// ===================== TICKETS =====================
async function ensureTicketsCategory(guild) {
  const existing = guild.channels.cache.find(
    (c) => c.type === ChannelType.GuildCategory && c.name === "vivid-tickets"
  );
  if (existing) return existing;
  return guild.channels.create({
    name: "vivid-tickets",
    type: ChannelType.GuildCategory,
  });
}

async function createOrGetTicketChannel(guild, userId) {
  const existing = await ticketsCol.findOne({ guildId: guild.id, userId, status: "open" });
  if (existing?.channelId) {
    const ch = await guild.channels.fetch(existing.channelId).catch(() => null);
    if (ch && ch.type === ChannelType.GuildText) return ch;
    await ticketsCol.updateOne(
      { guildId: guild.id, userId, status: "open" },
      { $set: { status: "stale", staleAt: new Date() } }
    );
  }
  const cfg = await getConfig(guild.id);
  const supportRoleId = cfg?.supportRoleId || null;
  const category = await ensureTicketsCategory(guild);
  const overwrites = [
    { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
    {
      id: userId,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
      ],
    },
    {
      id: client.user.id,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
        PermissionsBitField.Flags.ManageChannels,
      ],
    },
  ];
  if (supportRoleId) {
    overwrites.push({
      id: supportRoleId,
      allow: [
        PermissionsBitField.Flags.ViewChannel,
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.ReadMessageHistory,
      ],
    });
  }
  const ch = await guild.channels.create({
    name: `ticket-${userId}`,
    type: ChannelType.GuildText,
    parent: category.id,
    permissionOverwrites: overwrites,
  });
  await ticketsCol.insertOne({
    guildId: guild.id,
    channelId: ch.id,
    userId,
    status: "open",
    createdAt: new Date(),
  });
  return ch;
}

async function closeTicket(guild, channelId, userId, closerId) {
  const ch = await guild.channels.fetch(channelId).catch(() => null);
  if (!ch || ch.type !== ChannelType.GuildText) return;

  const embed = new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Ticket Closed`)
    .setDescription(`<@${closerId}> closed this ticket.`)
    .setFooter({ text: BRAND_FOOTER })
    .setTimestamp(new Date());

  await ch.send({ embeds: [embed] }).catch(() => {});
  await ch.delete().catch(() => {});

  await ticketsCol.updateOne(
    { guildId: guild.id, channelId, userId, status: "open" },
    { $set: { status: "closed", closedAt: new Date(), closedBy: closerId } }
  );
}

// ===================== TICKET PANEL (DROPDOWN) =====================
async function buildTicketPanelPayload(guildId) {
  const products = await productsCol
    .find({ guildId })
    .sort({ createdAt: -1 })
    .limit(25)
    .toArray();
  const embed = new EmbedBuilder()
    .setColor(BRAND_COLOR)
    .setTitle(`${BRAND_NAME} • Purchase Tickets`)
    .setDescription(
      products.length
        ? "Select what you want to buy. A private ticket will be created for purchase support + checkout."
        : "No products yet. Admins: /addproduct"
    )
    .setFooter({ text: BRAND_FOOTER });
  const components = [];
  if (products.length) {
    const menu = new StringSelectMenuBuilder()
      .setCustomId("ticket_product_select")
      .setPlaceholder("Select a product to open a purchase ticket")
      .setMinValues(1)
      .setMaxValues(1);
    for (const p of products) {
      menu.addOptions(
        new StringSelectMenuOptionBuilder()
          .setLabel(p.name.slice(0, 100))
          .setValue(p._id.toString())
          .setDescription((p.description || "Purchase support").slice(0, 100))
      );
    }
    components.push(new ActionRowBuilder().addComponents(menu));
  }
  return { embeds: [embed], components };
}

async function upsertTicketPanel(guild) {
  const ch = await guild.channels.fetch(TICKET_PANEL_CHANNEL_ID).catch(() => null);
  if (!ch || ch.type !== ChannelType.GuildText) return;
  const payload = await buildTicketPanelPayload(guild.id);
  const cfg = await getConfig(guild.id);
  if (cfg?.ticketPanelMessageId) {
    try {
      const msg = await ch.messages.fetch(cfg.ticketPanelMessageId);
      await msg.edit(payload);
      return;
    } catch {
      // recreate
    }
  }
  const msg = await ch.send(payload);
  await upsertConfig(guild.id, { ticketPanelMessageId: msg.id });
}

// ===================== STRIPE CHECKOUT (amount-based) =====================
function makeLineItemForProduct(product, planKey) {
  const plan = product.prices?.[planKey];
  if (!plan?.amountMinor) throw new Error("Plan not available");
  const isSub = planKey === "monthly" || planKey === "annual";
  const recurring = isSub ? PLAN_INTERVAL[planKey] : undefined;
  return {
    price_data: {
      currency: CURRENCY,
      unit_amount: plan.amountMinor,
      product_data: {
        name: `${BRAND_NAME} • ${product.name} (${PLAN_LABELS[planKey]})`,
        description: product.description,
      },
      ...(recurring ? { recurring } : {}),
    },
    quantity: 1,
  };
}

function makeLineItemForDonation(amountMinor) {
  return {
    price_data: {
      currency: CURRENCY,
      unit_amount: amountMinor,
      product_data: {
        name: `${BRAND_NAME} • Donation`,
        description: "Thank you for supporting Vivid Tweaks.",
      },
    },
    quantity: 1,
  };
}

async function createCheckoutSessionForProduct({ guildId, userId, product, planKey, purchaseId }) {
  const isSubscription = planKey === "monthly" || planKey === "annual";
  const lineItem = makeLineItemForProduct(product, planKey);
  const session = await stripe.checkout.sessions.create({
    mode: isSubscription ? "subscription" : "payment",
    line_items: [lineItem],
    success_url: `${PUBLIC_BASE_URL}/success`,
    cancel_url: `${PUBLIC_BASE_URL}/cancel`,
    metadata: {
      purchase_id: purchaseId,
      guild_id: guildId,
      user_id: userId,
      type: "product",
      product_id: product._id.toString(),
      product_name: product.name,
      plan_key: planKey,
      role_id: product.roleId,
      amount_minor: String(product.prices[planKey].amountMinor),
      currency: CURRENCY,
    },
  });
  await purchasesCol.insertOne({
    purchaseId,
    guildId,
    userId,
    type: "product",
    productId: product._id.toString(),
    productName: product.name,
    planKey,
    roleId: product.roleId,
    amountMinor: product.prices[planKey].amountMinor,
    currency: CURRENCY,
    stripeSessionId: session.id,
    status: "created",
    createdAt: new Date(),
  });
  return session.url;
}

async function createCheckoutSessionForDonation({ guildId, userId, amountMinor, purchaseId }) {
  const lineItem = makeLineItemForDonation(amountMinor);
  const session = await stripe.checkout.sessions.create({
    mode: "payment",
    line_items: [lineItem],
    success_url: `${PUBLIC_BASE_URL}/success`,
    cancel_url: `${PUBLIC_BASE_URL}/cancel`,
    metadata: {
      purchase_id: purchaseId,
      guild_id: guildId,
      user_id: userId,
      type: "donation",
      amount_minor: String(amountMinor),
      currency: CURRENCY,
    },
  });
  await purchasesCol.insertOne({
    purchaseId,
    guildId,
    userId,
    type: "donation",
    amountMinor,
    currency: CURRENCY,
    stripeSessionId: session.id,
    status: "created",
    createdAt: new Date(),
  });
  return session.url;
}

// ===================== REFUNDS (internal only) =====================
async function executeRefundInternal(purchase) {
  let refundId = null;
  if (purchase.stripePaymentIntentId) {
    const refund = await stripe.refunds.create({ payment_intent: purchase.stripePaymentIntentId });
    refundId = refund.id;
  } else if (purchase.stripeSubscriptionId) {
    const sub = await stripe.subscriptions.retrieve(purchase.stripeSubscriptionId, {
      expand: ["latest_invoice.payment_intent"],
    });
    const pi = sub.latest_invoice?.payment_intent;
    if (pi?.id) {
      const refund = await stripe.refunds.create({ payment_intent: pi.id });
      refundId = refund.id;
    }
    await stripe.subscriptions.cancel(purchase.stripeSubscriptionId).catch(() => {});
  } else {
    throw new Error("No Stripe payment references on record.");
  }
  return refundId;
}

// ===================== SLASH COMMANDS =====================
const commands = [
  new SlashCommandBuilder()
    .setName("addproduct")
    .setDescription("Add a product (amounts, not Stripe price IDs)")
    .addStringOption((o) => o.setName("name").setDescription("Product name").setRequired(true))
    .addStringOption((o) => o.setName("description").setDescription("Short description").setRequired(true))
    .addRoleOption((o) => o.setName("role").setDescription("Role to grant after payment").setRequired(true))
    .addStringOption((o) => o.setName("one_time").setDescription("One-time amount (e.g. 9.99)"))
    .addStringOption((o) => o.setName("monthly").setDescription("Monthly amount (e.g. 4.99)"))
    .addStringOption((o) => o.setName("annual").setDescription("Annual amount (e.g. 49.99)"))
    .addStringOption((o) => o.setName("lifetime").setDescription("Lifetime amount (e.g. 199.99)")),
  new SlashCommandBuilder()
    .setName("removeproduct")
    .setDescription("Remove a product by ID")
    .addStringOption((o) => o.setName("product_id").setDescription("Product ID").setRequired(true)),
  new SlashCommandBuilder()
    .setName("setprice")
    .setDescription("Set/disable a plan amount on an existing product")
    .addStringOption((o) => o.setName("product_id").setDescription("Product ID").setRequired(true))
    .addStringOption((o) =>
      o.setName("plan").setDescription("Plan").setRequired(true).addChoices(
        { name: "one_time", value: "one_time" },
        { name: "monthly", value: "monthly" },
        { name: "annual", value: "annual" },
        { name: "lifetime", value: "lifetime" }
      )
    )
    .addStringOption((o) => o.setName("amount").setDescription("Amount (e.g. 9.99) or 'none'").setRequired(true)),
  new SlashCommandBuilder()
    .setName("listproducts")
    .setDescription("List product IDs and enabled plans"),
  new SlashCommandBuilder()
    .setName("setthankschannel")
    .setDescription("Set channel for 'thanks for buying' embeds")
    .addChannelOption((o) => o.setName("channel").setDescription("Text channel").setRequired(true)),
  new SlashCommandBuilder()
    .setName("setsupportrole")
    .setDescription("Set support role for ticket channels (optional but smart)")
    .addRoleOption((o) => o.setName("role").setDescription("Support role").setRequired(true)),
  new SlashCommandBuilder()
    .setName("ticketpanel")
    .setDescription("Repost/update the ticket panel in the fixed panel channel"),
  new SlashCommandBuilder()
    .setName("donate")
    .setDescription("Donate (one-time)")
    .addStringOption((o) => o.setName("amount").setDescription("Amount (e.g. 5 or 9.99)").setRequired(true)),
  new SlashCommandBuilder()
    .setName("refund")
    .setDescription("Request a refund by Purchase ID (approval required; within 24h)")
    .addStringOption((o) => o.setName("purchase_id").setDescription("Purchase ID (VT-...)").setRequired(true)),
  new SlashCommandBuilder()
    .setName("cancelsub")
    .setDescription("Cancel your active subscription (keeps access until period ends)"),
].map((c) => c.toJSON());

async function registerCommands() {
  const rest = new REST({ version: "10" }).setToken(DISCORD_TOKEN);
  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, ALLOWED_GUILD_ID), { body: commands });
  console.log("✅ Guild slash commands registered");
}

// ===================== INTERACTIONS =====================
client.on("interactionCreate", async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (!ensureAllowedGuild(interaction)) return;

      if (interaction.commandName === "addproduct") {
        if (!requireAdmin(interaction)) return;
        const name = interaction.options.getString("name", true);
        const description = interaction.options.getString("description", true);
        const role = interaction.options.getRole("role", true);
        const raw = {
          one_time: interaction.options.getString("one_time"),
          monthly: interaction.options.getString("monthly"),
          annual: interaction.options.getString("annual"),
          lifetime: interaction.options.getString("lifetime"),
        };
        const prices = {};
        for (const k of PLAN_KEYS) {
          if (!raw[k]) continue;
          const minor = amountToMinorUnits(raw[k]);
          if (!minor) {
            return interaction.reply({ content: `Invalid amount for ${k}: "${raw[k]}"`, ephemeral: true });
          }
          prices[k] = { amountMinor: minor };
        }
        if (!enabledPlans(prices).length) {
          return interaction.reply({ content: "You must set at least one plan amount.", ephemeral: true });
        }
        const doc = await productsCol.insertOne({
          guildId: interaction.guild.id,
          name,
          description,
          roleId: role.id,
          prices,
          createdAt: new Date(),
        });
        await upsertTicketPanel(interaction.guild).catch(() => {});
        return interaction.reply({
          content: `Added **${name}** (ID: \`${doc.insertedId.toString()}\`) with plans: ${formatPlans(prices)}`,
          ephemeral: true,
        });
      }

      if (interaction.commandName === "removeproduct") {
        if (!requireAdmin(interaction)) return;
        const productId = interaction.options.getString("product_id", true);
        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product ID.", ephemeral: true });
        const result = await productsCol.deleteOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (result.deletedCount === 0) return interaction.reply({ content: "Product not found.", ephemeral: true });
        await upsertTicketPanel(interaction.guild).catch(() => {});
        return interaction.reply({ content: `Product \`${productId}\` removed.`, ephemeral: true });
      }

      if (interaction.commandName === "setprice") {
        if (!requireAdmin(interaction)) return;
        const productId = interaction.options.getString("product_id", true);
        const plan = interaction.options.getString("plan", true);
        const amountRaw = interaction.options.getString("amount", true).trim().toLowerCase();
        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product id.", ephemeral: true });
        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });
        if (amountRaw === "none" || amountRaw === "disable" || amountRaw === "off") {
          await productsCol.updateOne({ _id: new ObjectId(productId) }, { $unset: { [`prices.${plan}`]: "" } });
        } else {
          const minor = amountToMinorUnits(amountRaw);
          if (!minor) return interaction.reply({ content: "Invalid amount format. Example: 9.99", ephemeral: true });
          await productsCol.updateOne(
            { _id: new ObjectId(productId) },
            { $set: { [`prices.${plan}`]: { amountMinor: minor } } }
          );
        }
        await upsertTicketPanel(interaction.guild).catch(() => {});
        return interaction.reply({ content: `Updated plan **${plan}** for \`${productId}\`.`, ephemeral: true });
      }

      if (interaction.commandName === "listproducts") {
        if (!requireAdmin(interaction)) return;
        const products = await productsCol
          .find({ guildId: interaction.guild.id })
          .sort({ createdAt: -1 })
          .limit(50)
          .toArray();
        if (!products.length) return interaction.reply({ content: "No products.", ephemeral: true });
        const lines = products.map(
          (p) => `• **${p.name}** \`${p._id}\` | ${formatPlans(p.prices)} | role <@&${p.roleId}>`
        );
        return interaction.reply({ content: lines.join("\n").slice(0, 1900), ephemeral: true });
      }

      if (interaction.commandName === "setthankschannel") {
        if (!requireAdmin(interaction)) return;
        const ch = interaction.options.getChannel("channel", true);
        if (!ch || ch.type !== ChannelType.GuildText) {
          return interaction.reply({ content: "Pick a normal text channel.", ephemeral: true });
        }
        await upsertConfig(interaction.guild.id, { thanksChannelId: ch.id });
        return interaction.reply({ content: `Thanks channel set to <#${ch.id}>`, ephemeral: true });
      }

      if (interaction.commandName === "setsupportrole") {
        if (!requireAdmin(interaction)) return;
        const role = interaction.options.getRole("role", true);
        await upsertConfig(interaction.guild.id, { supportRoleId: role.id });
        return interaction.reply({ content: `Support role set to <@&${role.id}>`, ephemeral: true });
      }

      if (interaction.commandName === "ticketpanel") {
        if (!requireAdmin(interaction)) return;
        await upsertTicketPanel(interaction.guild).catch(() => {});
        return interaction.reply({ content: "Ticket panel updated.", ephemeral: true });
      }

      if (interaction.commandName === "donate") {
        const amountRaw = interaction.options.getString("amount", true);
        const minor = amountToMinorUnits(amountRaw);
        if (!minor) return interaction.reply({ content: "Invalid amount. Example: 5 or 9.99", ephemeral: true });
        if (minor < 100) return interaction.reply({ content: "Minimum donation is £1.00", ephemeral: true });
        const purchaseId = makePurchaseId();
        const url = await createCheckoutSessionForDonation({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          amountMinor: minor,
          purchaseId,
        });
        return interaction.reply({
          content: `Your donation checkout is ready (Purchase ID: \`${purchaseId}\`). Please check your DMs or this channel shortly.`,
          ephemeral: true,
        });
      }

      if (interaction.commandName === "refund") {
        if (!requireAdmin(interaction)) return;
        const purchaseId = interaction.options.getString("purchase_id", true).trim();
        const purchase = await purchasesCol.findOne({ purchaseId, guildId: interaction.guild.id });
        if (!purchase) return interaction.reply({ content: "Purchase not found.", ephemeral: true });
        if (purchase.status !== "paid") return interaction.reply({ content: `Purchase status is \`${purchase.status}\`.`, ephemeral: true });
        const paidAt = purchase.paidAt ? new Date(purchase.paidAt) : null;
        if (!paidAt) return interaction.reply({ content: "No paidAt recorded. Can't refund safely.", ephemeral: true });
        if (Date.now() - paidAt.getTime() > REFUND_WINDOW_MS) {
          return interaction.reply({ content: "Refund window expired (over 24 hours).", ephemeral: true });
        }
        const requestId = makeRefundRequestId();
        await refundRequestsCol.insertOne({
          requestId,
          purchaseId,
          guildId: interaction.guild.id,
          requestedBy: interaction.user.id,
          status: "pending",
          createdAt: new Date(),
        });
        const embed = new EmbedBuilder()
          .setColor(BRAND_COLOR)
          .setTitle(`${BRAND_NAME} • Refund Request`)
          .addFields(
            { name: "Request ID", value: `\`${requestId}\``, inline: true },
            { name: "Purchase ID", value: `\`${purchaseId}\``, inline: true },
            { name: "User", value: `<@${purchase.userId}>`, inline: true },
            { name: "Amount", value: minorToDisplay(purchase.amountMinor), inline: true },
            { name: "Requested by", value: `<@${interaction.user.id}>`, inline: true },
            { name: "Status", value: "`pending approval`", inline: true }
          )
          .setFooter({ text: BRAND_FOOTER })
          .setTimestamp(new Date());
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder()
            .setCustomId(`refund_approve:${requestId}`)
            .setStyle(ButtonStyle.Success)
            .setLabel("Approve"),
          new ButtonBuilder()
            .setCustomId(`refund_reject:${requestId}`)
            .setStyle(ButtonStyle.Danger)
            .setLabel("Reject")
        );
        await logToPurchaseLog(interaction.guild, embed, [row]);
        return interaction.reply({
          content: `Refund request created: \`${requestId}\` (awaiting approval).`,
          ephemeral: true,
        });
      }

      if (interaction.commandName === "cancelsub") {
        await interaction.deferReply({ ephemeral: true });
        const purchase = await purchasesCol.findOne(
          {
            guildId: interaction.guild.id,
            userId: interaction.user.id,
            type: "product",
            stripeSubscriptionId: { $exists: true, $ne: null },
            status: "paid",
            subscriptionEndedAt: { $exists: false },
          },
          { sort: { paidAt: -1 } }
        );
        if (!purchase) return interaction.editReply("You don’t have an active subscription purchase on record.");
        let sub;
        try {
          sub = await stripe.subscriptions.retrieve(purchase.stripeSubscriptionId);
        } catch (e) {
          console.error("Subscription retrieve failed:", e);
          return interaction.editReply("Couldn’t find your subscription in Stripe. Contact support.");
        }
        if (sub.status === "canceled") {
          await purchasesCol.updateMany(
            { stripeSubscriptionId: sub.id, status: "paid" },
            {
              $set: {
                subscriptionStatus: "canceled",
                subscriptionEndedAt: new Date(),
              },
            }
          );
          return interaction.editReply("Your subscription is already canceled.");
        }
        try {
          sub = await stripe.subscriptions.update(purchase.stripeSubscriptionId, {
            cancel_at_period_end: true,
          });
        } catch (e) {
          console.error("Cancel update failed:", e);
          return interaction.editReply("Stripe refused to cancel your subscription. Contact support.");
        }
        await purchasesCol.updateMany(
          { stripeSubscriptionId: sub.id, status: "paid" },
          {
            $set: {
              subscriptionStatus: sub.status,
              cancelAtPeriodEnd: !!sub.cancel_at_period_end,
              subscriptionCanceledAt: new Date(),
              currentPeriodEnd: sub.current_period_end ? new Date(sub.current_period_end * 1000) : null,
            },
          }
        );
        const embed = new EmbedBuilder()
          .setColor(BRAND_COLOR)
          .setTitle(`${BRAND_NAME} • Subscription Cancellation Requested`)
          .addFields(
            { name: "User", value: `<@${interaction.user.id}>`, inline: true },
            { name: "Purchase ID", value: `\`${purchase.purchaseId}\``, inline: true },
            { name: "Cancel at period end", value: "Yes", inline: true }
          )
          .setFooter({ text: BRAND_FOOTER })
          .setTimestamp(new Date());
        await logToPurchaseLog(interaction.guild, embed);
        return interaction.editReply("Subscription canceled. You keep access until the end of your billing period.");
      }
    }

    if (interaction.isStringSelectMenu()) {
      if (!ensureAllowedGuild(interaction)) return;
      if (interaction.customId === "ticket_product_select") {
        const productId = interaction.values[0];
        if (!ObjectId.isValid(productId)) {
          return interaction.reply({ content: "Invalid product selection.", ephemeral: true });
        }
        const product = await productsCol.findOne({
          _id: new ObjectId(productId),
          guildId: interaction.guild.id,
        });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });
        if (!enabledPlans(product.prices).length) return interaction.reply({ content: "No plans enabled for that product.", ephemeral: true });
        const ticketCh = await createOrGetTicketChannel(interaction.guild, interaction.user.id);
        const embed = new EmbedBuilder()
          .setColor(BRAND_COLOR)
          .setTitle(`${BRAND_NAME} • Purchase Ticket`)
          .setDescription(
            `User: <@${interaction.user.id}>\n` +
            `Product: **${product.name}**\n` +
            `${product.description}\n` +
            `Pick a plan below to generate your Stripe Checkout link.`
          )
          .addFields({ name: "Plans", value: formatPlans(product.prices), inline: false })
          .setFooter({ text: BRAND_FOOTER })
          .setTimestamp(new Date());
        await ticketCh.send({
          content: `<@${interaction.user.id}>`,
          embeds: [embed],
          components: planButtonsForTicket(product),
        });
        return interaction.reply({
          content: `Ticket ready: <#${ticketCh.id}>`,
          ephemeral: true,
        });
      }
    }

    if (interaction.isButton()) {
      if (!ensureAllowedGuild(interaction)) return;

      if (interaction.customId === "close_ticket") {
        const ticket = await ticketsCol.findOne({ guildId: interaction.guild.id, channelId: interaction.channelId, status: "open" });
        if (!ticket) return interaction.reply({ content: "This isn’t a valid open ticket.", ephemeral: true });
        if (interaction.user.id !== ticket.userId && !isAdmin(interaction)) {
          return interaction.reply({ content: "Only the ticket owner or server admins can close this ticket.", ephemeral: true });
        }
        await closeTicket(interaction.guild, interaction.channelId, ticket.userId, interaction.user.id);
        return interaction.reply({ content: "Ticket closed.", ephemeral: true });
      }

      if (interaction.customId.startsWith("tp:")) {
        const [, productId, planKey] = interaction.customId.split(":");
        if (!ObjectId.isValid(productId)) return interaction.reply({ content: "Invalid product.", ephemeral: true });
        if (!PLAN_KEYS.includes(planKey)) return interaction.reply({ content: "Invalid plan.", ephemeral: true });
        const ticket = await ticketsCol.findOne({ guildId: interaction.guild.id, channelId: interaction.channelId, status: "open" });
        if (!ticket) return interaction.reply({ content: "This isn’t a valid purchase ticket channel.", ephemeral: true });
        if (interaction.user.id !== ticket.userId) return interaction.reply({ content: "Only the ticket owner can generate checkout links.", ephemeral: true });
        const product = await productsCol.findOne({ _id: new ObjectId(productId), guildId: interaction.guild.id });
        if (!product) return interaction.reply({ content: "Product not found.", ephemeral: true });
        if (!product.prices?.[planKey]?.amountMinor) return interaction.reply({ content: "That plan is not available.", ephemeral: true });
        const purchaseId = makePurchaseId();
        const url = await createCheckoutSessionForProduct({
          guildId: interaction.guild.id,
          userId: interaction.user.id,
          product,
          planKey,
          purchaseId,
        });
        const embed = new EmbedBuilder()
          .setColor(BRAND_COLOR)
          .setTitle(`${BRAND_NAME} • Secure Checkout Ready`)
          .setDescription(
            `**Purchase ID:** \`${purchaseId}\`\n` +
            `**Plan:** ${PLAN_LABELS[planKey]}\n` +
            `Please complete your payment using the secure link below.`
          )
          .addFields({ name: "Checkout", value: "[Complete Payment on Stripe](<LINK>)".replace("<LINK>", url) })
          .setFooter({ text: BRAND_FOOTER })
          .setTimestamp(new Date());
        await interaction.channel.send({ embeds: [embed] });
        return interaction.reply({ content: "Checkout link sent securely in this ticket.", ephemeral: true });
      }

      if (interaction.customId.startsWith("refund_approve:") || interaction.customId.startsWith("refund_reject:")) {
        if (interaction.user.id !== REFUND_APPROVER_USER_ID) {
          return interaction.reply({ content: "You are not allowed to approve/reject refunds.", ephemeral: true });
        }
        const [action, requestId] = interaction.customId.split(":");
        const reqDoc = await refundRequestsCol.findOne({ requestId, guildId: interaction.guild.id });
        if (!reqDoc) return interaction.reply({ content: "Refund request not found.", ephemeral: true });
        if (reqDoc.status !== "pending") return interaction.reply({ content: `Request already \`${reqDoc.status}\`.`, ephemeral: true });
        const originalEmbed = interaction.message.embeds?.[0];
        const base = originalEmbed ? EmbedBuilder.from(originalEmbed) : new EmbedBuilder().setColor(BRAND_COLOR);
        if (action === "refund_reject") {
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "rejected", rejectedAt: new Date(), rejectedBy: interaction.user.id } }
          );
          const fields = (originalEmbed?.fields || []).filter((f) => f.name !== "Status");
          base.setFields(...fields, { name: "Status", value: "`rejected`", inline: true })
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());
          return interaction.update({ embeds: [base], components: [] });
        }

        await interaction.deferUpdate();
        const purchase = await purchasesCol.findOne({ purchaseId: reqDoc.purchaseId, guildId: interaction.guild.id });
        if (!purchase) {
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "failed", failedAt: new Date(), failureReason: "Purchase not found" } }
          );
          const fields = (originalEmbed?.fields || []).filter((f) => f.name !== "Status");
          base.setFields(...fields, { name: "Status", value: "`failed (missing purchase)`", inline: true })
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());
          await interaction.editReply({ embeds: [base], components: [] }).catch(() => {});
          return;
        }
        const paidAt = purchase.paidAt ? new Date(purchase.paidAt) : null;
        if (!paidAt || Date.now() - paidAt.getTime() > REFUND_WINDOW_MS) {
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "failed", failedAt: new Date(), failureReason: "Refund window expired" } }
          );
          const fields = (originalEmbed?.fields || []).filter((f) => f.name !== "Status");
          base.setFields(...fields, { name: "Status", value: "`failed (window expired)`", inline: true })
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());
          await interaction.editReply({ embeds: [base], components: [] }).catch(() => {});
          return;
        }
        try {
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "approved", approvedAt: new Date(), approvedBy: interaction.user.id } }
          );
          const refundId = await executeRefundInternal(purchase);
          if (purchase.type === "product" && purchase.roleId) {
            const member = await interaction.guild.members.fetch(purchase.userId).catch(() => null);
            if (member) await member.roles.remove(purchase.roleId, `Refund approved (${requestId})`).catch(() => {});
          }
          await purchasesCol.updateOne(
            { purchaseId: purchase.purchaseId },
            { $set: { status: "refunded", refundedAt: new Date(), stripeRefundId: refundId } }
          );
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "executed", executedAt: new Date() } }
          );
          const fields = (originalEmbed?.fields || []).filter((f) => f.name !== "Status");
          base.setFields(...fields, { name: "Status", value: "`refunded`", inline: true })
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());
          await interaction.editReply({ embeds: [base], components: [] }).catch(() => {});
        } catch (e) {
          console.error("Refund execution failed:", e);
          await refundRequestsCol.updateOne(
            { requestId },
            { $set: { status: "failed", failedAt: new Date(), failureReason: String(e.message || e).slice(0, 200) } }
          );
          const fields = (originalEmbed?.fields || []).filter((f) => f.name !== "Status");
          base.setFields(...fields, { name: "Status", value: "`failed`", inline: true })
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());
          await interaction.editReply({ embeds: [base], components: [] }).catch(() => {});
        }
      }
    }
  } catch (e) {
    console.error(e);
    if (interaction.isRepliable()) {
      interaction.reply({ content: "Something broke. Shocking.", ephemeral: true }).catch(() => {});
    }
  }
});

// ===================== READY =====================
client.on("ready", async () => {
  console.log(`🤖 Logged in as ${client.user.tag}`);
  const guild = await client.guilds.fetch(ALLOWED_GUILD_ID).catch(() => null);
  if (!guild) return;
  await ensurePurchaseLogChannel(guild).catch(() => {});
  await upsertTicketPanel(guild).catch(() => {});
});

// ===================== WEB SERVER (Stripe Webhook) =====================
const app = express();
app.post("/stripe/webhook", bodyParser.raw({ type: "application/json" }), async (req, res) => {
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers["stripe-signature"], STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error("❌ Webhook signature failed:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }
  try {
    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const meta = session.metadata || {};
      const purchaseId = meta.purchase_id;
      const guildId = meta.guild_id;
      const userId = meta.user_id;
      const type = meta.type;
      if (!purchaseId || !guildId || !userId || !type) return res.json({ received: true });
      if (guildId !== ALLOWED_GUILD_ID) return res.json({ received: true });

      const paymentIntentId = session.payment_intent || null;
      const subscriptionId = session.subscription || null;
      let currentPeriodEnd = null;
      let subscriptionStatus = null;
      let cancelAtPeriodEnd = null;
      if (subscriptionId) {
        const sub = await stripe.subscriptions.retrieve(subscriptionId);
        currentPeriodEnd = sub.current_period_end ? new Date(sub.current_period_end * 1000) : null;
        subscriptionStatus = sub.status;
        cancelAtPeriodEnd = !!sub.cancel_at_period_end;
      }

      const updated = await purchasesCol.findOneAndUpdate(
        { purchaseId, stripeSessionId: session.id },
        {
          $set: {
            status: "paid",
            paidAt: new Date(),
            stripePaymentIntentId: paymentIntentId,
            stripeSubscriptionId: subscriptionId,
            currentPeriodEnd,
            subscriptionStatus,
            cancelAtPeriodEnd,
          },
        },
        { returnDocument: "after" }
      );
      if (!updated.value) {
        await purchasesCol.updateOne(
          { stripeSessionId: session.id },
          {
            $setOnInsert: {
              purchaseId,
              guildId,
              userId,
              type,
              amountMinor: Number(meta.amount_minor || 0) || 0,
              currency: meta.currency || CURRENCY,
              stripeSessionId: session.id,
              createdAt: new Date(),
            },
            $set: {
              status: "paid",
              paidAt: new Date(),
              stripePaymentIntentId: paymentIntentId,
              stripeSubscriptionId: subscriptionId,
              currentPeriodEnd,
              subscriptionStatus,
              cancelAtPeriodEnd,
            },
          },
          { upsert: true }
        );
      }

      const guild = await client.guilds.fetch(guildId).catch(() => null);
      if (!guild) return res.json({ received: true });

      if (type === "product" && meta.role_id) {
        try {
          const member = await guild.members.fetch(userId);
          await member.roles.add(meta.role_id, `Stripe paid: ${purchaseId}`);
        } catch (e) {
          console.error("Role grant failed:", e);
        }
      }

      const amountMinor = Number(meta.amount_minor || 0) || 0;
      const planKey = meta.plan_key || null;
      const logEmbed = new EmbedBuilder()
        .setColor(BRAND_COLOR)
        .setTitle(`${BRAND_NAME} • Purchase Completed`)
        .addFields(
          { name: "User", value: `<@${userId}>`, inline: true },
          { name: "Purchase ID", value: `\`${purchaseId}\``, inline: true },
          { name: "Type", value: type, inline: true },
          { name: "Amount", value: amountMinor ? minorToDisplay(amountMinor) : "n/a", inline: true },
          ...(planKey ? [{ name: "Plan", value: PLAN_LABELS[planKey] || planKey, inline: true }] : []),
          ...(meta.product_name ? [{ name: "Product", value: meta.product_name, inline: false }] : [])
        )
        .setFooter({ text: BRAND_FOOTER })
        .setTimestamp(new Date());
      await logToPurchaseLog(guild, logEmbed);
      if (type === "product") {
        await sendThanksIfConfigured(guild, userId, purchaseId);
      }
      return res.json({ received: true });
    }

    if (event.type === "customer.subscription.updated") {
      const sub = event.data.object;
      await purchasesCol.updateMany(
        { stripeSubscriptionId: sub.id, status: "paid" },
        {
          $set: {
            subscriptionStatus: sub.status,
            cancelAtPeriodEnd: !!sub.cancel_at_period_end,
            currentPeriodEnd: sub.current_period_end ? new Date(sub.current_period_end * 1000) : null,
            subscriptionUpdatedAt: new Date(),
          },
        }
      );
      return res.json({ received: true });
    }

    if (event.type === "customer.subscription.deleted") {
      const sub = event.data.object;
      const purchase = await purchasesCol.findOne(
        { stripeSubscriptionId: sub.id, status: "paid", type: "product" },
        { sort: { paidAt: -1 } }
      );
      if (purchase) {
        const guild = await client.guilds.fetch(purchase.guildId).catch(() => null);
        if (guild) {
          try {
            const member = await guild.members.fetch(purchase.userId);
            if (purchase.roleId) {
              await member.roles.remove(purchase.roleId, "Subscription ended");
            }
          } catch (e) {
            console.error("Role removal failed:", e);
          }
          const embed = new EmbedBuilder()
            .setColor(BRAND_COLOR)
            .setTitle(`${BRAND_NAME} • Subscription Ended`)
            .addFields(
              { name: "User", value: `<@${purchase.userId}>`, inline: true },
              { name: "Purchase ID", value: `\`${purchase.purchaseId}\``, inline: true },
              { name: "Role removed", value: purchase.roleId ? `<@&${purchase.roleId}>` : "n/a", inline: false }
            )
            .setFooter({ text: BRAND_FOOTER })
            .setTimestamp(new Date());
          await logToPurchaseLog(guild, embed);
        }
      }
      await purchasesCol.updateMany(
        { stripeSubscriptionId: sub.id, status: "paid" },
        { $set: { subscriptionStatus: "canceled", subscriptionEndedAt: new Date() } }
      );
      return res.json({ received: true });
    }
    return res.json({ received: true });
  } catch (err) {
    console.error("Webhook handler error:", err);
    return res.status(500).send("Webhook handler failed");
  }
});

app.get("/success", (req, res) => res.status(200).send("Payment successful. You can close this tab."));
app.get("/cancel", (req, res) => res.status(200).send("Payment canceled."));

// ===================== START =====================
(async () => {
  await initDb();
  await registerCommands();
  await client.login(DISCORD_TOKEN);
  app.listen(PORT, "0.0.0.0", () => console.log(`🌐 Web server on :${PORT}`));
})();
